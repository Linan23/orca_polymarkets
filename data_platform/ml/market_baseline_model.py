"""Grouped time-aware market ML models plus whale-signal analysis."""

from __future__ import annotations

import csv
import json
import math
import pickle
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_platform.ml.market_dataset_builder import (
    ANALYSIS_COLUMNS,
    COVERAGE_FEATURE_COLUMNS,
    DATASET_VERSION,
    DEFAULT_DATASET_PATH,
    FEATURE_COLUMNS,
    GROUP_KEY_COLUMN,
    PRICE_BASELINE_COLUMN,
    RECENT_TRUSTED_WHALE_FEATURE_COLUMNS,
    RESOLUTION_EDGE_COLUMN,
    SCORED_WHALE_PRESSURE_FEATURE_COLUMNS,
    STATIC_METADATA_FEATURE_COLUMNS,
    TARGET_COLUMN,
    WHALE_FEATURE_COLUMNS,
)


TASK_MARKET_OUTCOME = "market_outcome"
TASK_WHALE_SIGNAL = "whale_signal"
TASK_MARKET_MOVEMENT_12H = "market_movement_12h"
TASK_MARKET_MOVEMENT_24H = "market_movement_24h"
PRIMARY_ESTIMATOR_TYPE = "lightgbm"
REGIME_ALL = "all"
REGIME_TRADE_COVERED = "trade_covered"
REGIME_COLD_START = "cold_start"
MODEL_VERSION = "market_outcome_random_forest_v2"
LIGHTGBM_MODEL_VERSION = "market_outcome_lightgbm_v1"
DEFAULT_MODEL_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_model.pkl")
DEFAULT_METRICS_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_metrics.json")
DEFAULT_IMPORTANCE_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_feature_importance.csv")
DEFAULT_COMPARISON_PATH = Path("data_platform/runtime/ml/market_feature_set_comparison.json")
DEFAULT_LIGHTGBM_FEATURE_SET_COMPARISON_PATH = Path("data_platform/runtime/ml/market_feature_set_comparison_lightgbm.json")
DEFAULT_LIGHTGBM_MODEL_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_model.pkl")
DEFAULT_LIGHTGBM_METRICS_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_metrics.json")
DEFAULT_LIGHTGBM_IMPORTANCE_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_feature_importance.csv")
DEFAULT_MODEL_FAMILY_COMPARISON_PATH = Path("data_platform/runtime/ml/market_model_family_comparison.json")
DEFAULT_TRAINING_REPORT_PATH = Path("data_platform/runtime/ml/market_model_training_report.json")
DEFAULT_WHALE_SIGNAL_ANALYSIS_PATH = Path("data_platform/runtime/ml/market_whale_signal_analysis.json")
DEFAULT_MOVEMENT_FEATURE_SET_COMPARISON_PATH = Path("data_platform/runtime/ml/market_movement_feature_set_comparison.json")
DEFAULT_MOVEMENT_TUNING_REPORT_PATH = Path("data_platform/runtime/ml/market_movement_tuning_report.json")
DEFAULT_WEEK10_11_MOVEMENT_REPORT_PATH = Path("data_platform/runtime/ml/week10_11_market_movement_report.md")
DEFAULT_WHALE_FEATURE_ABLATION_REPORT_PATH = Path("data_platform/runtime/ml/market_whale_feature_ablation_report.json")
DEFAULT_MOVEMENT_RESIDUAL_REPORT_PATH = Path("data_platform/runtime/ml/market_movement_residual_report.json")
DEFAULT_WEEK10_11_RESIDUAL_REPORT_PATH = Path("data_platform/runtime/ml/week10_11_market_movement_residual_report.md")
ROLLING_MIN_TRAIN_FRACTION = 0.5
ROLLING_TEST_WINDOW_FRACTION = 0.15
PRICE_SATURATION_THRESHOLD = 0.98
END_TIME_BUCKETING_STRATEGY = "exact_market_end_time"
MIN_ROLLING_RMSE_LIFT = 0.0001
FEATURE_SELECTION_NONE = "none"
FEATURE_SELECTION_TRAINING_CORRELATION = "training_correlation"
FEATURE_SELECTION_MODES = {FEATURE_SELECTION_NONE, FEATURE_SELECTION_TRAINING_CORRELATION}
FEATURE_SELECTION_MIN_ABS_CORRELATION = 0.015
FEATURE_SELECTION_MAX_WHALE_FEATURES = 24
RESIDUAL_SELECTOR_THRESHOLDS = (0.01, 0.02, 0.05)
RESIDUAL_SELECTOR_MAX_FEATURES = (8, 16, 24)
RESIDUAL_RANDOM_FOREST_PARAMS = {
    "n_estimators": 120,
    "max_depth": 3,
    "min_samples_leaf": 12,
    "max_features": "sqrt",
}
RESIDUAL_LIGHTGBM_PARAMS = {
    "n_estimators": 180,
    "learning_rate": 0.03,
    "num_leaves": 15,
    "max_depth": 4,
    "min_child_samples": 12,
    "subsample": 0.75,
    "colsample_bytree": 0.75,
    "reg_lambda": 5.0,
    "reg_alpha": 0.25,
}
MOVEMENT_TUNING_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "profile": "rf_shallow",
        "estimator_type": "random_forest",
        "description": "Small random forest with shallow trees to reduce overfit against sparse whale features.",
        "params": {
            "n_estimators": 120,
            "max_depth": 3,
            "min_samples_leaf": 12,
            "max_features": "sqrt",
        },
    },
    {
        "profile": "rf_shallow_selected_whale",
        "estimator_type": "random_forest",
        "description": "Shallow random forest with train-fold whale feature selection to reject noisy whale columns.",
        "feature_selection": FEATURE_SELECTION_TRAINING_CORRELATION,
        "params": {
            "n_estimators": 120,
            "max_depth": 3,
            "min_samples_leaf": 12,
            "max_features": "sqrt",
        },
    },
    {
        "profile": "rf_regularized",
        "estimator_type": "random_forest",
        "description": "Moderately regularized random forest for whale movement tuning.",
        "params": {
            "n_estimators": 180,
            "max_depth": 5,
            "min_samples_leaf": 8,
            "max_features": "sqrt",
        },
    },
    {
        "profile": "rf_current",
        "estimator_type": "random_forest",
        "description": "Current random-forest defaults used by the baseline trainer.",
        "params": {},
    },
    {
        "profile": "lgbm_regularized",
        "estimator_type": "lightgbm",
        "description": "Regularized LightGBM profile for movement prediction if LightGBM is available.",
        "params": {
            "n_estimators": 180,
            "learning_rate": 0.03,
            "num_leaves": 15,
            "max_depth": 4,
            "min_child_samples": 12,
            "subsample": 0.75,
            "colsample_bytree": 0.75,
            "reg_lambda": 5.0,
            "reg_alpha": 0.25,
        },
    },
)
HORIZON_BAND_DEFINITIONS = (
    {
        "name": "far_168h_plus",
        "label": "168h_plus",
        "min_horizon_hours": 168.0,
        "max_horizon_hours": None,
        "max_horizon_hours_exclusive": None,
    },
    {
        "name": "mid_72h_to_167h",
        "label": "72h_to_167h",
        "min_horizon_hours": 72.0,
        "max_horizon_hours": None,
        "max_horizon_hours_exclusive": 168.0,
    },
    {
        "name": "near_under_72h",
        "label": "under_72h",
        "min_horizon_hours": None,
        "max_horizon_hours": None,
        "max_horizon_hours_exclusive": 72.0,
    },
)

TIME_CONTEXT_FEATURE_COLUMNS = (
    "horizon_hours",
    "hours_to_close",
    "market_age_hours",
    "market_duration_hours",
)
PRICE_FEATURE_COLUMNS = (
    "last_price_side",
    "last_price_opposite",
    "avg_price_side",
    "avg_price_opposite",
    "min_price_side",
    "min_price_opposite",
    "max_price_side",
    "max_price_opposite",
    "price_gap_side_minus_opposite",
    "price_abs_distance_from_even",
)
PRICE_BASELINE_FEATURE_COLUMNS = TIME_CONTEXT_FEATURE_COLUMNS + COVERAGE_FEATURE_COLUMNS + PRICE_FEATURE_COLUMNS
EXPERIMENTAL_SCORED_WHALE_PRESSURE_COLUMNS = set(SCORED_WHALE_PRESSURE_FEATURE_COLUMNS)
DEFAULT_WHALE_FEATURE_COLUMNS = tuple(
    column for column in WHALE_FEATURE_COLUMNS if column not in EXPERIMENTAL_SCORED_WHALE_PRESSURE_COLUMNS
)
WHALE_ONLY_FEATURE_COLUMNS = TIME_CONTEXT_FEATURE_COLUMNS + COVERAGE_FEATURE_COLUMNS + DEFAULT_WHALE_FEATURE_COLUMNS
PRICE_PLUS_WHALE_FEATURE_COLUMNS = PRICE_BASELINE_FEATURE_COLUMNS + DEFAULT_WHALE_FEATURE_COLUMNS
FEATURE_SET_COLUMNS: dict[str, tuple[str, ...]] = {
    "full": FEATURE_COLUMNS,
    "price_only": PRICE_BASELINE_FEATURE_COLUMNS,
    "whale_only": WHALE_ONLY_FEATURE_COLUMNS,
    "price_plus_whale": PRICE_PLUS_WHALE_FEATURE_COLUMNS,
    "cold_start": TIME_CONTEXT_FEATURE_COLUMNS + STATIC_METADATA_FEATURE_COLUMNS,
}
TASK_DEFAULT_FEATURE_SET = {
    TASK_MARKET_OUTCOME: "full",
    TASK_WHALE_SIGNAL: "price_plus_whale",
    TASK_MARKET_MOVEMENT_12H: "price_plus_whale",
    TASK_MARKET_MOVEMENT_24H: "price_plus_whale",
}
REGRESSION_TASKS = {TASK_WHALE_SIGNAL, TASK_MARKET_MOVEMENT_12H, TASK_MARKET_MOVEMENT_24H}
REGIME_METADATA: dict[str, dict[str, str]] = {
    REGIME_ALL: {
        "label": "all_rows",
        "description": "All exported rows, including both trade-covered and cold-start observation windows.",
    },
    REGIME_TRADE_COVERED: {
        "label": "with_any_trade_before_cutoff",
        "description": "Only rows with at least one pre-cutoff trade; this is the regime where whale lift can be judged.",
    },
    REGIME_COLD_START: {
        "label": "no_trade_before_cutoff",
        "description": "Only rows with no pre-cutoff trades; treat this as a separate cold-start problem.",
    },
}
REGIME_ANALYSIS_ORDER = (REGIME_TRADE_COVERED, REGIME_COLD_START)


def _dedupe_columns(columns: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Return columns in first-seen order without duplicates."""
    seen: set[str] = set()
    deduped: list[str] = []
    for column in columns:
        if column in seen:
            continue
        seen.add(column)
        deduped.append(column)
    return tuple(deduped)


def _load_training_rows(dataset_path: Path) -> list[dict[str, Any]]:
    """Load CSV rows and coerce numeric features for model training."""
    rows: list[dict[str, Any]] = []
    with dataset_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            parsed: dict[str, Any] = dict(row)
            for column in FEATURE_COLUMNS:
                parsed[column] = float(row.get(column, 0) or 0)
            parsed[TARGET_COLUMN] = int(float(row.get(TARGET_COLUMN, 0) or 0))
            last_price_value = row.get("last_price_side")
            avg_price_side = float(row.get("avg_price_side", 0) or 0)
            last_price_side = float(last_price_value or 0)
            if PRICE_BASELINE_COLUMN in row and str(row.get(PRICE_BASELINE_COLUMN, "")).strip() != "":
                parsed[PRICE_BASELINE_COLUMN] = float(row.get(PRICE_BASELINE_COLUMN, 0) or 0)
            else:
                parsed[PRICE_BASELINE_COLUMN] = last_price_side if str(last_price_value or "").strip() != "" else avg_price_side
            if RESOLUTION_EDGE_COLUMN in row and str(row.get(RESOLUTION_EDGE_COLUMN, "")).strip() != "":
                parsed[RESOLUTION_EDGE_COLUMN] = float(row.get(RESOLUTION_EDGE_COLUMN, 0) or 0)
            else:
                parsed[RESOLUTION_EDGE_COLUMN] = round(float(parsed[TARGET_COLUMN]) - float(parsed[PRICE_BASELINE_COLUMN]), 8)
            for column in ANALYSIS_COLUMNS:
                parsed[column] = float(parsed.get(column, row.get(column, 0)) or 0)
            parsed["market_end_time"] = datetime.fromisoformat(str(row["market_end_time"]))
            parsed["horizon_hours"] = float(row.get("horizon_hours", 0) or 0)
            rows.append(parsed)
    return rows


def _require_ml_dependencies() -> None:
    """Ensure scikit-learn is importable before model work begins."""
    try:
        import sklearn  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for market ML training. Run `pip install -r requirements.txt` first."
        ) from exc


def _normalize_task(task: str) -> str:
    """Normalize and validate the task name."""
    normalized = str(task).strip().lower()
    if normalized not in {TASK_MARKET_OUTCOME, TASK_WHALE_SIGNAL, TASK_MARKET_MOVEMENT_12H, TASK_MARKET_MOVEMENT_24H}:
        raise RuntimeError(f"Unsupported task: {task}")
    return normalized


def _normalize_estimator_type(estimator_type: str) -> str:
    """Normalize and validate the estimator family."""
    normalized = str(estimator_type).strip().lower()
    if normalized not in {"random_forest", "lightgbm"}:
        raise RuntimeError(f"Unsupported estimator_type: {estimator_type}")
    return normalized


def _normalize_regime(regime: str | None) -> str:
    """Normalize and validate the regime selector."""
    normalized = REGIME_ALL if regime is None else str(regime).strip().lower()
    if normalized not in REGIME_METADATA:
        raise RuntimeError(f"Unsupported regime: {regime}")
    return normalized


def _normalize_feature_selection(feature_selection: str | None) -> str:
    """Normalize and validate the feature-selection mode."""
    normalized = FEATURE_SELECTION_NONE if feature_selection is None else str(feature_selection).strip().lower()
    if normalized not in FEATURE_SELECTION_MODES:
        raise RuntimeError(f"Unsupported feature_selection: {feature_selection}")
    return normalized


def _default_feature_set_for_context(task: str, regime: str) -> str:
    """Return the default feature set for one task/regime pair."""
    task = _normalize_task(task)
    regime = _normalize_regime(regime)
    if task == TASK_MARKET_OUTCOME and regime == REGIME_COLD_START:
        return "cold_start"
    return TASK_DEFAULT_FEATURE_SET[task]


def _resolve_feature_set(task: str, feature_set: str | None, regime: str = REGIME_ALL) -> str:
    """Return the requested feature set or the task/regime default."""
    task = _normalize_task(task)
    regime = _normalize_regime(regime)
    selected = _default_feature_set_for_context(task, regime) if feature_set is None else str(feature_set).strip().lower()
    if selected not in FEATURE_SET_COLUMNS:
        raise RuntimeError(f"Unsupported feature_set: {feature_set}")
    return selected


def _resolve_feature_columns(
    task: str,
    feature_set: str | None,
    regime: str = REGIME_ALL,
) -> tuple[str, tuple[str, ...]]:
    """Return the normalized feature-set name and columns for one task."""
    task = _normalize_task(task)
    resolved_feature_set = _resolve_feature_set(task, feature_set, regime)
    if task in REGRESSION_TASKS and resolved_feature_set not in {"price_only", "whale_only", "price_plus_whale"}:
        raise RuntimeError(f"{task} supports only price_only, whale_only, or price_plus_whale feature sets.")
    return resolved_feature_set, FEATURE_SET_COLUMNS[resolved_feature_set]


def _resolve_model_version(task: str, estimator_type: str, feature_set: str) -> str:
    """Return a stable model version string for one task/family/feature-set tuple."""
    task = _normalize_task(task)
    estimator_type = _normalize_estimator_type(estimator_type)
    feature_set = _resolve_feature_set(task, feature_set)
    known_versions = {
        (TASK_MARKET_OUTCOME, "random_forest", "full"): MODEL_VERSION,
        (TASK_MARKET_OUTCOME, "lightgbm", "full"): LIGHTGBM_MODEL_VERSION,
        (TASK_MARKET_OUTCOME, "random_forest", "price_only"): "market_price_only_random_forest_v1",
        (TASK_MARKET_OUTCOME, "random_forest", "price_plus_whale"): "market_price_plus_whale_random_forest_v1",
        (TASK_MARKET_OUTCOME, "lightgbm", "price_only"): "market_price_only_lightgbm_v1",
        (TASK_MARKET_OUTCOME, "lightgbm", "price_plus_whale"): "market_price_plus_whale_lightgbm_v1",
        (TASK_MARKET_OUTCOME, "random_forest", "whale_only"): "market_whale_only_random_forest_v1",
        (TASK_MARKET_OUTCOME, "lightgbm", "whale_only"): "market_whale_only_lightgbm_v1",
        (TASK_MARKET_OUTCOME, "random_forest", "cold_start"): "market_cold_start_random_forest_v1",
        (TASK_MARKET_OUTCOME, "lightgbm", "cold_start"): "market_cold_start_lightgbm_v1",
        (TASK_WHALE_SIGNAL, "random_forest", "price_only"): "market_whale_signal_price_only_random_forest_v1",
        (TASK_WHALE_SIGNAL, "random_forest", "whale_only"): "market_whale_signal_whale_only_random_forest_v1",
        (TASK_WHALE_SIGNAL, "random_forest", "price_plus_whale"): "market_whale_signal_price_plus_whale_random_forest_v1",
        (TASK_WHALE_SIGNAL, "lightgbm", "price_only"): "market_whale_signal_price_only_lightgbm_v1",
        (TASK_WHALE_SIGNAL, "lightgbm", "whale_only"): "market_whale_signal_whale_only_lightgbm_v1",
        (TASK_WHALE_SIGNAL, "lightgbm", "price_plus_whale"): "market_whale_signal_price_plus_whale_lightgbm_v1",
    }
    version = known_versions.get((task, estimator_type, feature_set))
    if version is not None:
        return version
    return f"{task}_{feature_set}_{estimator_type}_v1"


def _target_column_for_task(task: str) -> str:
    """Return the target column associated with one task."""
    task = _normalize_task(task)
    if task == TASK_MARKET_OUTCOME:
        return TARGET_COLUMN
    if task == TASK_WHALE_SIGNAL:
        return RESOLUTION_EDGE_COLUMN
    if task == TASK_MARKET_MOVEMENT_12H:
        return "future_price_delta_12h"
    return "future_price_delta_24h"


def _movement_required_columns(task: str) -> tuple[str, str]:
    """Return required target/observation columns for one movement task."""
    task = _normalize_task(task)
    if task == TASK_MARKET_MOVEMENT_12H:
        return "future_price_delta_12h", "future_price_observed_12h"
    if task == TASK_MARKET_MOVEMENT_24H:
        return "future_price_delta_24h", "future_price_observed_24h"
    raise RuntimeError(f"{task} is not a movement task.")


def _csv_fieldnames(dataset_path: Path) -> set[str]:
    """Return the CSV header columns for a dataset path."""
    with dataset_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return set(reader.fieldnames or [])


def _require_valid_movement_targets(
    *,
    dataset_path: Path,
    rows: list[dict[str, Any]],
    task: str,
) -> None:
    """Fail fast when a stale dataset lacks real 12h/24h movement labels."""
    target_column, observed_column = _movement_required_columns(task)
    missing_columns = sorted({target_column, observed_column} - _csv_fieldnames(dataset_path))
    if missing_columns:
        raise RuntimeError(
            f"{dataset_path} is missing movement target columns {missing_columns}. "
            "Re-run data_platform/jobs/export_market_ml_dataset.py before movement tuning."
        )
    observed_count = sum(1 for row in rows if float(row.get(observed_column, 0) or 0) >= 0.5)
    if observed_count == 0:
        raise RuntimeError(
            f"{dataset_path} has no observed rows for {target_column}. "
            "Movement models need future price observations in the selected 12h/24h window."
        )


def _filter_rows_by_horizon(
    rows: list[dict[str, Any]],
    *,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    max_horizon_hours_exclusive: float | None = None,
) -> list[dict[str, Any]]:
    """Filter rows to a horizon range when requested."""
    if min_horizon_hours is None and max_horizon_hours is None and max_horizon_hours_exclusive is None:
        return list(rows)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        horizon_hours = float(row["horizon_hours"])
        if min_horizon_hours is not None and horizon_hours < float(min_horizon_hours):
            continue
        if max_horizon_hours is not None and horizon_hours > float(max_horizon_hours):
            continue
        if max_horizon_hours_exclusive is not None and horizon_hours >= float(max_horizon_hours_exclusive):
            continue
        filtered.append(row)
    return filtered


def _filter_rows_by_regime(rows: list[dict[str, Any]], regime: str | None) -> list[dict[str, Any]]:
    """Filter rows to the requested regime."""
    normalized_regime = _normalize_regime(regime)
    if normalized_regime == REGIME_ALL:
        return list(rows)
    if normalized_regime == REGIME_TRADE_COVERED:
        return [row for row in rows if float(row["has_any_trade_before_cutoff"]) >= 0.5]
    return [row for row in rows if float(row["has_any_trade_before_cutoff"]) < 0.5]


def _regime_summary_context(regime: str) -> dict[str, str]:
    """Return serializable metadata for one regime."""
    normalized_regime = _normalize_regime(regime)
    metadata = REGIME_METADATA[normalized_regime]
    return {
        "regime": normalized_regime,
        "label": metadata["label"],
        "description": metadata["description"],
    }


def _ordered_end_time_buckets(rows: list[dict[str, Any]]) -> list[tuple[datetime, list[str]]]:
    """Return exact end-time buckets, each containing all condition refs that resolve together."""
    grouped_end_times: dict[str, datetime] = {}
    for row in rows:
        condition_ref = str(row[GROUP_KEY_COLUMN])
        market_end_time = row["market_end_time"]
        existing = grouped_end_times.get(condition_ref)
        if existing is None or market_end_time < existing:
            grouped_end_times[condition_ref] = market_end_time
    buckets: dict[datetime, set[str]] = {}
    for condition_ref, market_end_time in grouped_end_times.items():
        buckets.setdefault(market_end_time, set()).add(condition_ref)
    ordered_buckets = [
        (bucket_time, sorted(condition_refs))
        for bucket_time, condition_refs in sorted(buckets.items(), key=lambda item: item[0])
    ]
    if len(ordered_buckets) < 2:
        raise RuntimeError("At least two distinct market_end_time buckets are required for grouped evaluation.")
    return ordered_buckets


def _flatten_bucket_conditions(buckets: list[tuple[datetime, list[str]]]) -> list[str]:
    """Return the condition refs contained in an ordered slice of end-time buckets."""
    flattened: list[str] = []
    for _, condition_refs in buckets:
        flattened.extend(condition_refs)
    return flattened


def _grouped_time_split(
    rows: list[dict[str, Any]],
    train_fraction: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[str]]:
    """Split rows by end-time buckets, keeping older buckets in train and newer buckets in test."""
    if not 0 < train_fraction < 1:
        raise RuntimeError("--train-fraction must be between 0 and 1.")
    ordered_buckets = _ordered_end_time_buckets(rows)
    train_bucket_count = int(len(ordered_buckets) * train_fraction)
    train_bucket_count = max(1, min(train_bucket_count, len(ordered_buckets) - 1))
    train_conditions = _flatten_bucket_conditions(ordered_buckets[:train_bucket_count])
    test_conditions = _flatten_bucket_conditions(ordered_buckets[train_bucket_count:])
    train_condition_set = set(train_conditions)
    test_condition_set = set(test_conditions)
    train_rows = [row for row in rows if str(row[GROUP_KEY_COLUMN]) in train_condition_set]
    test_rows = [row for row in rows if str(row[GROUP_KEY_COLUMN]) in test_condition_set]
    return train_rows, test_rows, train_conditions, test_conditions


def _build_rolling_splits(
    rows: list[dict[str, Any]],
    *,
    min_train_fraction: float = ROLLING_MIN_TRAIN_FRACTION,
    test_window_fraction: float = ROLLING_TEST_WINDOW_FRACTION,
) -> tuple[list[dict[str, Any]], int, int]:
    """Return grouped rolling split definitions plus end-time bucket metadata."""
    if not 0 < min_train_fraction < 1:
        raise RuntimeError("Rolling min_train_fraction must be between 0 and 1.")
    if not 0 < test_window_fraction < 1:
        raise RuntimeError("Rolling test_window_fraction must be between 0 and 1.")

    ordered_buckets = _ordered_end_time_buckets(rows)
    group_count = len(ordered_buckets)
    min_train_groups = max(1, math.ceil(group_count * min_train_fraction))
    configured_window_size = max(3, math.ceil(group_count * test_window_fraction))
    available_test_groups = max(group_count - min_train_groups, 1)
    actual_window_size = min(configured_window_size, available_test_groups)

    splits: list[dict[str, Any]] = []
    start_index = min_train_groups
    fold_index = 1
    while start_index < group_count:
        end_index = min(group_count, start_index + actual_window_size)
        train_buckets = ordered_buckets[:start_index]
        test_buckets = ordered_buckets[start_index:end_index]
        train_conditions = _flatten_bucket_conditions(train_buckets)
        test_conditions = _flatten_bucket_conditions(test_buckets)
        if not test_conditions:
            break
        train_condition_set = set(train_conditions)
        test_condition_set = set(test_conditions)
        splits.append(
            {
                "fold_index": fold_index,
                "train_bucket_count": len(train_buckets),
                "test_bucket_count": len(test_buckets),
                "train_conditions": train_conditions,
                "test_conditions": test_conditions,
                "train_rows": [row for row in rows if str(row[GROUP_KEY_COLUMN]) in train_condition_set],
                "test_rows": [row for row in rows if str(row[GROUP_KEY_COLUMN]) in test_condition_set],
            }
        )
        fold_index += 1
        start_index += actual_window_size

    if not splits:
        raise RuntimeError("Grouped rolling evaluation requires enough conditions for at least one test window.")
    return splits, configured_window_size, actual_window_size


def _split_time_range(rows: list[dict[str, Any]]) -> dict[str, str]:
    """Return the ISO min/max end-time range for one row slice."""
    return {
        "min": min(row["market_end_time"] for row in rows).isoformat(),
        "max": max(row["market_end_time"] for row in rows).isoformat(),
    }


def _feature_matrix(rows: list[dict[str, Any]], feature_columns: tuple[str, ...]) -> list[list[float]]:
    """Convert model rows into a feature matrix."""
    return [[float(row[column]) for column in feature_columns] for row in rows]


def _targets(rows: list[dict[str, Any]], task: str) -> list[float]:
    """Return the target vector for one task."""
    target_column = _target_column_for_task(task)
    if task == TASK_MARKET_OUTCOME:
        return [int(row[target_column]) for row in rows]
    return [float(row[target_column]) for row in rows]


def _safe_abs_pearson(feature_values: list[float], target_values: list[float]) -> float:
    """Return absolute Pearson correlation, falling back to zero for constant inputs."""
    if len(feature_values) < 2 or len(feature_values) != len(target_values):
        return 0.0
    feature_mean = sum(feature_values) / len(feature_values)
    target_mean = sum(target_values) / len(target_values)
    centered_features = [value - feature_mean for value in feature_values]
    centered_targets = [value - target_mean for value in target_values]
    feature_variance = sum(value * value for value in centered_features)
    target_variance = sum(value * value for value in centered_targets)
    denominator = math.sqrt(feature_variance * target_variance)
    if denominator <= 0:
        return 0.0
    covariance = sum(
        feature_value * target_value
        for feature_value, target_value in zip(centered_features, centered_targets, strict=True)
    )
    return abs(float(covariance) / denominator)


def _training_correlation_feature_selection(
    *,
    train_rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    task: str,
    target_values: list[float] | None = None,
    min_abs_correlation: float = FEATURE_SELECTION_MIN_ABS_CORRELATION,
    max_selected_whale_features: int = FEATURE_SELECTION_MAX_WHALE_FEATURES,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Select whale columns on the training fold while preserving non-whale baseline columns."""
    if task not in REGRESSION_TASKS:
        raise RuntimeError("training_correlation feature selection is supported only for regression tasks.")
    candidate_columns = tuple(column for column in feature_columns if column in WHALE_FEATURE_COLUMNS)
    candidate_column_set = set(candidate_columns)
    always_keep_columns = tuple(column for column in feature_columns if column not in candidate_column_set)
    target_values = target_values if target_values is not None else _targets(train_rows, task)
    if len(target_values) != len(train_rows):
        raise RuntimeError("Feature-selection target_values length must match train_rows.")
    candidate_scores: list[dict[str, Any]] = []
    for index, column in enumerate(candidate_columns):
        values = [float(row.get(column, 0.0) or 0.0) for row in train_rows]
        nonzero_count = sum(1 for value in values if abs(value) > 1e-12)
        unique_count = len({round(value, 10) for value in values})
        candidate_scores.append(
            {
                "feature": column,
                "original_index": index,
                "abs_correlation": _safe_abs_pearson(values, target_values),
                "nonzero_fraction": round(nonzero_count / len(values), 6) if values else 0.0,
                "unique_count": unique_count,
                "is_constant": unique_count <= 1,
            }
        )

    ranked_candidates = sorted(
        candidate_scores,
        key=lambda item: (
            -float(item["abs_correlation"]),
            -float(item["nonzero_fraction"]),
            int(item["original_index"]),
        ),
    )
    selected_candidate_list = [
        str(item["feature"])
        for item in ranked_candidates
        if not bool(item["is_constant"]) and float(item["abs_correlation"]) >= min_abs_correlation
    ]
    if max_selected_whale_features > 0:
        selected_candidate_list = selected_candidate_list[:max_selected_whale_features]
    selected_candidate_names = set(selected_candidate_list)

    selected_columns = tuple(
        column
        for column in feature_columns
        if column not in candidate_column_set or column in selected_candidate_names
    )
    selected_ranked = [
        item for item in ranked_candidates if str(item["feature"]) in selected_candidate_names
    ]
    dropped_ranked = [
        item for item in ranked_candidates if str(item["feature"]) not in selected_candidate_names
    ]
    summary = {
        "mode": FEATURE_SELECTION_TRAINING_CORRELATION,
        "requested_feature_count": len(feature_columns),
        "selected_feature_count": len(selected_columns),
        "always_kept_feature_count": len(always_keep_columns),
        "candidate_whale_feature_count": len(candidate_columns),
        "selected_whale_feature_count": len(selected_candidate_names),
        "dropped_whale_feature_count": len(candidate_columns) - len(selected_candidate_names),
        "min_abs_correlation": min_abs_correlation,
        "max_selected_whale_features": max_selected_whale_features,
        "selected_whale_feature_names": selected_candidate_list,
        "selected_whale_features": [
            {
                "feature": str(item["feature"]),
                "abs_correlation": round(float(item["abs_correlation"]), 6),
                "nonzero_fraction": item["nonzero_fraction"],
                "unique_count": item["unique_count"],
            }
            for item in selected_ranked[:12]
        ],
        "dropped_whale_features": [
            {
                "feature": str(item["feature"]),
                "abs_correlation": round(float(item["abs_correlation"]), 6),
                "nonzero_fraction": item["nonzero_fraction"],
                "unique_count": item["unique_count"],
            }
            for item in dropped_ranked[:12]
        ],
    }
    return selected_columns, summary


def _select_features_for_training_fold(
    *,
    train_rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    task: str,
    feature_selection: str,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Resolve feature columns for one train/test fold."""
    feature_selection = _normalize_feature_selection(feature_selection)
    if feature_selection == FEATURE_SELECTION_NONE:
        return feature_columns, {
            "mode": FEATURE_SELECTION_NONE,
            "requested_feature_count": len(feature_columns),
            "selected_feature_count": len(feature_columns),
            "candidate_whale_feature_count": sum(1 for column in feature_columns if column in WHALE_FEATURE_COLUMNS),
            "selected_whale_feature_count": sum(1 for column in feature_columns if column in WHALE_FEATURE_COLUMNS),
            "dropped_whale_feature_count": 0,
        }
    return _training_correlation_feature_selection(
        train_rows=train_rows,
        feature_columns=feature_columns,
        task=task,
    )


def _build_baseline_model(task: str) -> Any:
    """Return the baseline estimator for one task."""
    task = _normalize_task(task)
    if task == TASK_MARKET_OUTCOME:
        from sklearn.dummy import DummyClassifier

        return DummyClassifier(strategy="most_frequent")

    from sklearn.dummy import DummyRegressor

    return DummyRegressor(strategy="constant", constant=0.0)


def _build_estimator(
    task: str,
    estimator_type: str,
    random_state: int,
    estimator_params: dict[str, Any] | None = None,
) -> Any:
    """Return a configured estimator for one task and model family."""
    task = _normalize_task(task)
    estimator_type = _normalize_estimator_type(estimator_type)
    estimator_params = estimator_params or {}
    if estimator_type == "random_forest":
        if task == TASK_MARKET_OUTCOME:
            from sklearn.ensemble import RandomForestClassifier

            params = {
                "n_estimators": 300,
                "max_depth": 8,
                "min_samples_leaf": 3,
                "random_state": random_state,
                "class_weight": "balanced_subsample",
                "n_jobs": -1,
            }
            params.update(estimator_params)
            return RandomForestClassifier(**params)

        from sklearn.ensemble import RandomForestRegressor

        params = {
            "n_estimators": 300,
            "max_depth": 8,
            "min_samples_leaf": 3,
            "random_state": random_state,
            "n_jobs": -1,
        }
        params.update(estimator_params)
        return RandomForestRegressor(**params)

    try:
        if task == TASK_MARKET_OUTCOME:
            from lightgbm import LGBMClassifier

            params = {
                "objective": "binary",
                "n_estimators": 300,
                "learning_rate": 0.05,
                "num_leaves": 31,
                "max_depth": 6,
                "min_child_samples": 5,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "reg_lambda": 1.0,
                "class_weight": "balanced",
                "random_state": random_state,
                "n_jobs": -1,
                "importance_type": "gain",
                "verbosity": -1,
            }
            params.update(estimator_params)
            return LGBMClassifier(**params)

        from lightgbm import LGBMRegressor

        params = {
            "objective": "regression",
            "n_estimators": 300,
            "learning_rate": 0.05,
            "num_leaves": 31,
            "max_depth": 6,
            "min_child_samples": 5,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_lambda": 1.0,
            "random_state": random_state,
            "n_jobs": -1,
            "importance_type": "gain",
            "verbosity": -1,
        }
        params.update(estimator_params)
        return LGBMRegressor(**params)
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "lightgbm is required for this model path. Run `pip install -r requirements.txt` first."
        ) from exc


def _price_rule_predictions(rows: list[dict[str, Any]]) -> list[int]:
    """Return the deterministic price-ordering prediction for each row."""
    return [int(float(row["avg_price_side"]) > float(row["avg_price_opposite"])) for row in rows]


def _class_balance(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Return class counts from the outcome label regardless of the model task."""
    side_wins = sum(int(row[TARGET_COLUMN]) for row in rows)
    return {
        "side_wins": side_wins,
        "side_loses": len(rows) - side_wins,
    }


def _price_rule_accuracy(rows: list[dict[str, Any]]) -> float:
    """Return the deterministic avg-price ordering accuracy on the supplied rows."""
    if not rows:
        return 0.0
    predictions = _price_rule_predictions(rows)
    labels = [int(row[TARGET_COLUMN]) for row in rows]
    correct = sum(int(prediction == label) for prediction, label in zip(predictions, labels, strict=True))
    return round(correct / len(rows), 6)


def _relative_to_price_rule_summary(summary: dict[str, Any]) -> dict[str, float | None]:
    """Return one model summary expressed relative to the deterministic price rule."""
    price_rule_accuracy = float(summary.get("price_rule_accuracy") or 0.0)
    model_accuracy = summary.get("accuracy")
    return {
        "price_rule_accuracy": price_rule_accuracy,
        "model_accuracy": round(float(model_accuracy), 6) if model_accuracy is not None else None,
        "accuracy_delta": round(float(model_accuracy) - price_rule_accuracy, 6) if model_accuracy is not None else None,
    }


def _safe_roc_auc(y_true: list[int], probabilities: list[float]) -> float | None:
    """Return ROC-AUC when both classes are present."""
    if len(set(y_true)) < 2:
        return None
    from sklearn.metrics import roc_auc_score

    return round(float(roc_auc_score(y_true, probabilities)), 6)


def _safe_log_loss(y_true: list[int], probabilities: list[float]) -> float | None:
    """Return binary log loss when both classes are present."""
    if len(set(y_true)) < 2:
        return None
    from sklearn.metrics import log_loss

    clipped = [min(max(float(value), 1e-9), 1 - 1e-9) for value in probabilities]
    return round(float(log_loss(y_true, clipped, labels=[0, 1])), 6)


def _per_horizon_metrics(
    rows: list[dict[str, Any]],
    *,
    task: str,
    predictions: list[float],
    probabilities: list[float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return task-aware metrics broken down by horizon."""

    grouped: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        grouped.setdefault(str(int(float(row["horizon_hours"]))), []).append(index)

    metrics_by_horizon: dict[str, dict[str, Any]] = {}
    for horizon, indices in grouped.items():
        subset_rows = [rows[index] for index in indices]
        subset_predictions = [predictions[index] for index in indices]
        subset_probabilities = [float(probabilities[index]) for index in indices] if probabilities is not None else None
        metrics_by_horizon[horizon] = _subset_metrics(
            subset_rows,
            task=task,
            predictions=subset_predictions,
            probabilities=subset_probabilities,
        )
    return metrics_by_horizon


def _subset_metrics(
    rows: list[dict[str, Any]],
    *,
    task: str,
    predictions: list[float],
    probabilities: list[float] | None = None,
) -> dict[str, Any]:
    """Return task-aware metrics for an arbitrary row subset."""
    from sklearn.metrics import accuracy_score, mean_absolute_error, mean_squared_error

    price_rule_accuracy = _price_rule_accuracy(rows)
    entry: dict[str, Any] = {
        "row_count": len(rows),
        "class_balance": _class_balance(rows),
        "price_rule_accuracy": price_rule_accuracy,
        "price_saturated": price_rule_accuracy > PRICE_SATURATION_THRESHOLD,
    }
    if task == TASK_MARKET_OUTCOME:
        y_true = [int(row[TARGET_COLUMN]) for row in rows]
        y_pred = [int(value) for value in predictions]
        accuracy = round(float(accuracy_score(y_true, y_pred)), 6)
        entry["accuracy"] = accuracy
        entry["accuracy_vs_price_rule_delta"] = round(accuracy - price_rule_accuracy, 6)
        if probabilities is not None:
            entry["roc_auc"] = _safe_roc_auc(y_true, [float(value) for value in probabilities])
            entry["log_loss"] = _safe_log_loss(y_true, [float(value) for value in probabilities])
        return entry

    target_column = _target_column_for_task(task)
    y_true = [float(row[target_column]) for row in rows]
    entry["mae"] = round(float(mean_absolute_error(y_true, predictions)), 6)
    entry["rmse"] = round(float(math.sqrt(mean_squared_error(y_true, predictions))), 6)
    entry["mean_target_value"] = round(sum(y_true) / len(y_true), 6)
    return entry


def _coverage_segment_metrics(
    rows: list[dict[str, Any]],
    *,
    task: str,
    predictions: list[float],
    probabilities: list[float] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return sparse-row diagnostics for key coverage segments."""
    segment_groups = {
        "trade_coverage": {
            "with_any_trade": [index for index, row in enumerate(rows) if float(row["has_any_trade_before_cutoff"]) >= 0.5],
            "no_trade_before_cutoff": [index for index, row in enumerate(rows) if float(row["has_any_trade_before_cutoff"]) < 0.5],
        },
        "outcome_coverage": {
            "both_outcomes_observed": [
                index for index, row in enumerate(rows) if float(row["has_both_outcomes_before_cutoff"]) >= 0.5
            ],
            "missing_outcome_side_before_cutoff": [
                index for index, row in enumerate(rows) if float(row["has_both_outcomes_before_cutoff"]) < 0.5
            ],
        },
        "price_observation": {
            "both_side_prices_observed": [
                index
                for index, row in enumerate(rows)
                if float(row["side_price_observed"]) >= 0.5 and float(row["opposite_price_observed"]) >= 0.5
            ],
            "missing_side_price": [
                index
                for index, row in enumerate(rows)
                if not (float(row["side_price_observed"]) >= 0.5 and float(row["opposite_price_observed"]) >= 0.5)
            ],
        },
    }

    segmented_metrics: dict[str, dict[str, Any]] = {}
    for segment_name, segment_values in segment_groups.items():
        segment_summary: dict[str, Any] = {}
        for value_name, indices in segment_values.items():
            if not indices:
                continue
            subset_rows = [rows[index] for index in indices]
            subset_predictions = [predictions[index] for index in indices]
            subset_probabilities = [float(probabilities[index]) for index in indices] if probabilities is not None else None
            segment_summary[value_name] = _subset_metrics(
                subset_rows,
                task=task,
                predictions=subset_predictions,
                probabilities=subset_probabilities,
            )
        segmented_metrics[segment_name] = segment_summary
    return segmented_metrics


def _feature_importance_rows(model: Any, feature_columns: tuple[str, ...]) -> list[tuple[str, float]]:
    """Return sorted feature importances from a fitted estimator."""
    raw_importances = getattr(model, "feature_importances_", None)
    if raw_importances is None:
        return [(feature, 0.0) for feature in feature_columns]
    importances = list(zip(feature_columns, raw_importances, strict=True))
    importances.sort(key=lambda item: float(item[1]), reverse=True)
    return [(feature, float(importance)) for feature, importance in importances]


def _rolling_feature_selection_summary(folds: list[dict[str, Any]]) -> dict[str, Any]:
    """Return compact feature-selection counts across rolling folds."""
    selections = [
        fold["feature_selection"]
        for fold in folds
        if isinstance(fold.get("feature_selection"), dict)
    ]
    if not selections:
        return {"mode": FEATURE_SELECTION_NONE}
    selected_counts = [int(selection.get("selected_feature_count", 0)) for selection in selections]
    selected_whale_counts = [
        int(selection.get("selected_whale_feature_count", 0)) for selection in selections
    ]
    return {
        "mode": selections[0].get("mode", FEATURE_SELECTION_NONE),
        "fold_count": len(selections),
        "requested_feature_count": selections[0].get("requested_feature_count"),
        "min_selected_feature_count": min(selected_counts),
        "max_selected_feature_count": max(selected_counts),
        "average_selected_feature_count": round(sum(selected_counts) / len(selected_counts), 6),
        "min_selected_whale_feature_count": min(selected_whale_counts),
        "max_selected_whale_feature_count": max(selected_whale_counts),
        "average_selected_whale_feature_count": round(
            sum(selected_whale_counts) / len(selected_whale_counts),
            6,
        ),
    }


def _evaluate_split(
    *,
    train_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    feature_set: str,
    model_version: str,
    task: str,
    estimator_type: str,
    random_state: int,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> tuple[Any, dict[str, Any], list[tuple[str, float]]]:
    """Fit one model on a fixed grouped split and return metrics plus feature importances."""
    from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, mean_squared_error, precision_score, r2_score, recall_score

    task = _normalize_task(task)
    estimator_type = _normalize_estimator_type(estimator_type)
    feature_selection = _normalize_feature_selection(feature_selection)
    selected_feature_columns, feature_selection_summary = _select_features_for_training_fold(
        train_rows=train_rows,
        feature_columns=feature_columns,
        task=task,
        feature_selection=feature_selection,
    )
    x_train = _feature_matrix(train_rows, selected_feature_columns)
    x_test = _feature_matrix(test_rows, selected_feature_columns)
    y_train = _targets(train_rows, task)
    y_test = _targets(test_rows, task)

    if task == TASK_MARKET_OUTCOME and (len(set(y_train)) < 2 or len(set(y_test)) < 2):
        raise RuntimeError("Grouped time split must leave both target classes present in train and test.")

    baseline_model = _build_baseline_model(task)
    baseline_model.fit(x_train, y_train)
    baseline_predictions = baseline_model.predict(x_test)
    model = _build_estimator(task, estimator_type, random_state, estimator_params=estimator_params)
    model.fit(x_train, y_train)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMClassifier was fitted with feature names",
            category=UserWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMRegressor was fitted with feature names",
            category=UserWarning,
        )
        predictions = model.predict(x_test)
        probabilities = model.predict_proba(x_test)[:, 1] if task == TASK_MARKET_OUTCOME else None
    probability_values = [float(value) for value in probabilities] if probabilities is not None else None

    feature_importances = _feature_importance_rows(model, selected_feature_columns)
    slice_metrics = _subset_metrics(
        test_rows,
        task=task,
        predictions=[float(value) for value in predictions],
        probabilities=probability_values,
    )
    metrics: dict[str, Any] = {
        "dataset_version": DATASET_VERSION,
        "model_version": model_version,
        "target_column": _target_column_for_task(task),
        "task": task,
        "feature_set": feature_set,
        "feature_columns": list(selected_feature_columns),
        "feature_count": len(selected_feature_columns),
        "requested_feature_columns": list(feature_columns),
        "requested_feature_count": len(feature_columns),
        "feature_selection": feature_selection_summary,
        "estimator_type": estimator_type,
        "estimator_profile": estimator_profile,
        "estimator_params": estimator_params or {},
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        **slice_metrics,
        "per_horizon_metrics": _per_horizon_metrics(
            test_rows,
            task=task,
            predictions=[float(value) for value in predictions],
            probabilities=probability_values,
        ),
        "coverage_segment_metrics": _coverage_segment_metrics(
            test_rows,
            task=task,
            predictions=[float(value) for value in predictions],
            probabilities=probability_values,
        ),
        "top_features": [
            {"feature": feature, "importance": round(float(importance), 6)}
            for feature, importance in feature_importances[:10]
        ],
    }

    if task == TASK_MARKET_OUTCOME:
        metrics.update(
            {
                "baseline_accuracy": round(float(accuracy_score(y_test, baseline_predictions)), 6),
                "accuracy": round(float(accuracy_score(y_test, predictions)), 6),
                "baseline_vs_price_rule_delta": round(
                    float(accuracy_score(y_test, baseline_predictions)) - float(metrics["price_rule_accuracy"]),
                    6,
                ),
                "precision": round(float(precision_score(y_test, predictions, zero_division=0)), 6),
                "recall": round(float(recall_score(y_test, predictions, zero_division=0)), 6),
                "f1": round(float(f1_score(y_test, predictions, zero_division=0)), 6),
                "roc_auc": _safe_roc_auc([int(value) for value in y_test], probability_values or []),
                "log_loss": _safe_log_loss([int(value) for value in y_test], probability_values or []),
            }
        )
    else:
        zero_predictions = [0.0 for _ in y_test]
        metrics.update(
            {
                "baseline_mae": round(float(mean_absolute_error(y_test, zero_predictions)), 6),
                "baseline_rmse": round(float(math.sqrt(mean_squared_error(y_test, zero_predictions))), 6),
                "mae": round(float(mean_absolute_error(y_test, predictions)), 6),
                "rmse": round(float(math.sqrt(mean_squared_error(y_test, predictions))), 6),
                "r2": round(float(r2_score(y_test, predictions)), 6),
            }
        )
    return model, metrics, feature_importances


def _rolling_metric_summary(folds: list[dict[str, Any]], *, task: str) -> dict[str, Any]:
    """Aggregate rolling metrics across all folds."""
    if task == TASK_MARKET_OUTCOME:
        numeric_keys = (
            "baseline_accuracy",
            "accuracy",
            "accuracy_vs_price_rule_delta",
            "precision",
            "recall",
            "f1",
            "roc_auc",
            "log_loss",
            "price_rule_accuracy",
        )
    else:
        numeric_keys = ("baseline_mae", "baseline_rmse", "mae", "rmse", "r2", "price_rule_accuracy")

    averages: dict[str, Any] = {}
    for key in numeric_keys:
        values = [float(fold[key]) for fold in folds if fold.get(key) is not None]
        averages[key] = round(sum(values) / len(values), 6) if values else None
    averages["price_saturated_folds"] = sum(1 for fold in folds if bool(fold.get("price_saturated")))
    averages["price_saturated_fraction"] = round(
        averages["price_saturated_folds"] / len(folds),
        6,
    )
    return averages


def _transition_ready_from_rolling(
    *,
    random_forest_rolling: dict[str, Any],
    lightgbm_rolling: dict[str, Any],
) -> bool:
    """Return whether LightGBM clears the rolling transition gate."""
    return (
        PRIMARY_ESTIMATOR_TYPE == "lightgbm"
        and lightgbm_rolling.get("roc_auc") is not None
        and random_forest_rolling.get("roc_auc") is not None
        and lightgbm_rolling.get("log_loss") is not None
        and random_forest_rolling.get("log_loss") is not None
        and float(lightgbm_rolling["roc_auc"]) >= float(random_forest_rolling["roc_auc"])
        and float(lightgbm_rolling["log_loss"]) <= float(random_forest_rolling["log_loss"])
    )


def _rolling_evaluation(
    rows: list[dict[str, Any]],
    *,
    feature_columns: tuple[str, ...],
    feature_set: str,
    model_version: str,
    task: str,
    estimator_type: str,
    random_state: int,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> dict[str, Any]:
    """Evaluate one model family over grouped rolling windows."""
    feature_selection = _normalize_feature_selection(feature_selection)
    split_definitions, configured_window_size, actual_window_size = _build_rolling_splits(rows)
    folds: list[dict[str, Any]] = []
    for split_definition in split_definitions:
        _, fold_metrics, _ = _evaluate_split(
            train_rows=split_definition["train_rows"],
            test_rows=split_definition["test_rows"],
            feature_columns=feature_columns,
            feature_set=feature_set,
            model_version=model_version,
            task=task,
            estimator_type=estimator_type,
            random_state=random_state,
            estimator_params=estimator_params,
            estimator_profile=estimator_profile,
            feature_selection=feature_selection,
        )
        fold_summary: dict[str, Any] = {
            "fold_index": split_definition["fold_index"],
            "train_rows": len(split_definition["train_rows"]),
            "test_rows": len(split_definition["test_rows"]),
            "train_condition_count": len(split_definition["train_conditions"]),
            "test_condition_count": len(split_definition["test_conditions"]),
            "train_end_time_bucket_count": split_definition["train_bucket_count"],
            "test_end_time_bucket_count": split_definition["test_bucket_count"],
            "train_end_time_range": _split_time_range(split_definition["train_rows"]),
            "test_end_time_range": _split_time_range(split_definition["test_rows"]),
            "price_rule_accuracy": fold_metrics["price_rule_accuracy"],
            "price_saturated": fold_metrics["price_saturated"],
            "class_balance": fold_metrics["class_balance"],
            "per_horizon_metrics": fold_metrics["per_horizon_metrics"],
            "feature_count": fold_metrics.get("feature_count"),
            "requested_feature_count": fold_metrics.get("requested_feature_count"),
            "feature_selection": fold_metrics.get("feature_selection"),
        }
        if task == TASK_MARKET_OUTCOME:
            for key in (
                "baseline_accuracy",
                "accuracy",
                "accuracy_vs_price_rule_delta",
                "precision",
                "recall",
                "f1",
                "roc_auc",
                "log_loss",
            ):
                fold_summary[key] = fold_metrics.get(key)
        else:
            for key in ("baseline_mae", "baseline_rmse", "mae", "rmse", "r2"):
                fold_summary[key] = fold_metrics.get(key)
        folds.append(fold_summary)

    return {
        "split_unit": "market_end_time_bucket",
        "end_time_bucket_strategy": END_TIME_BUCKETING_STRATEGY,
        "min_train_fraction": ROLLING_MIN_TRAIN_FRACTION,
        "configured_test_window_size": configured_window_size,
        "test_window_size": actual_window_size,
        "step_size": actual_window_size,
        "fold_count": len(folds),
        "estimator_profile": estimator_profile,
        "estimator_params": estimator_params or {},
        "feature_selection": _rolling_feature_selection_summary(folds),
        "average": _rolling_metric_summary(folds, task=task),
        "folds": folds,
    }


def _assess_market_model(
    *,
    dataset_path: Path,
    feature_columns: tuple[str, ...],
    feature_set: str,
    model_version: str,
    task: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    include_rolling_metrics: bool = False,
    regime: str = REGIME_ALL,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> tuple[Any, dict[str, Any], list[tuple[str, float]]]:
    """Run one market-model assessment from a dataset path."""
    _require_ml_dependencies()
    rows = _filter_rows_by_regime(
        _filter_rows_by_horizon(
        _load_training_rows(dataset_path),
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        ),
        regime,
    )
    return _assess_market_model_rows(
        rows=rows,
        dataset_path=dataset_path,
        feature_columns=feature_columns,
        feature_set=feature_set,
        model_version=model_version,
        task=task,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        include_rolling_metrics=include_rolling_metrics,
        regime=regime,
        estimator_params=estimator_params,
        estimator_profile=estimator_profile,
        feature_selection=feature_selection,
    )


def _assess_market_model_rows(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    feature_columns: tuple[str, ...],
    feature_set: str,
    model_version: str,
    task: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    include_rolling_metrics: bool = False,
    regime: str = REGIME_ALL,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> tuple[Any, dict[str, Any], list[tuple[str, float]]]:
    """Run one market-model assessment from an in-memory filtered row slice."""
    _require_ml_dependencies()
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path} after horizon filtering.")
    task = _normalize_task(task)
    if task in {TASK_MARKET_MOVEMENT_12H, TASK_MARKET_MOVEMENT_24H}:
        _require_valid_movement_targets(dataset_path=dataset_path, rows=rows, task=task)
    resolved_regime = _normalize_regime(regime)
    feature_selection = _normalize_feature_selection(feature_selection)

    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    model, metrics, feature_importances = _evaluate_split(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=feature_columns,
        feature_set=feature_set,
        model_version=model_version,
        task=task,
        estimator_type=estimator_type,
        random_state=random_state,
        estimator_params=estimator_params,
        estimator_profile=estimator_profile,
        feature_selection=feature_selection,
    )
    metrics.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dataset_path": str(dataset_path),
            **_regime_summary_context(resolved_regime),
            "row_count": len(rows),
            "train_rows": len(train_rows),
            "test_rows": len(test_rows),
            "train_condition_count": len(train_conditions),
            "test_condition_count": len(test_conditions),
            "train_end_time_bucket_count": len({row["market_end_time"] for row in train_rows}),
            "test_end_time_bucket_count": len({row["market_end_time"] for row in test_rows}),
            "train_fraction": train_fraction,
            "random_state": random_state,
            "estimator_profile": estimator_profile,
            "estimator_params": estimator_params or {},
            "feature_selection_mode": feature_selection,
            "min_horizon_hours": min_horizon_hours,
            "max_horizon_hours": max_horizon_hours,
            "split_unit": "market_end_time_bucket",
            "end_time_bucket_strategy": END_TIME_BUCKETING_STRATEGY,
            "bucketed_end_time_boundary_respected": max(row["market_end_time"] for row in train_rows)
            < min(row["market_end_time"] for row in test_rows),
            "train_end_time_range": _split_time_range(train_rows),
            "test_end_time_range": _split_time_range(test_rows),
        }
    )
    if include_rolling_metrics:
        metrics["rolling_metrics"] = _rolling_evaluation(
            rows,
            feature_columns=feature_columns,
            feature_set=feature_set,
            model_version=model_version,
            task=task,
            estimator_type=estimator_type,
            random_state=random_state,
            estimator_params=estimator_params,
            estimator_profile=estimator_profile,
            feature_selection=feature_selection,
        )
    return model, metrics, feature_importances


def _regime_model_analysis(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    feature_columns: tuple[str, ...],
    feature_set: str,
    model_version: str,
    task: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    include_rolling_metrics: bool = False,
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> dict[str, dict[str, Any]]:
    """Return compact model summaries for the trade-covered and cold-start regimes."""
    analysis: dict[str, dict[str, Any]] = {}
    for regime in REGIME_ANALYSIS_ORDER:
        regime_rows = _filter_rows_by_regime(rows, regime)
        regime_feature_set = feature_set
        regime_feature_columns = feature_columns
        regime_model_version = model_version
        if task == TASK_MARKET_OUTCOME and feature_set == "full":
            regime_feature_set, regime_feature_columns = _resolve_feature_columns(task, None, regime)
            regime_model_version = _resolve_model_version(task, estimator_type, regime_feature_set)
        regime_summary: dict[str, Any] = {
            **_regime_summary_context(regime),
            "row_count": len(regime_rows),
            "feature_set": regime_feature_set,
        }
        if not regime_rows:
            regime_summary.update({"available": False, "reason": "No rows matched this regime."})
            analysis[regime] = regime_summary
            continue
        try:
            _, metrics, _ = _assess_market_model_rows(
                rows=regime_rows,
                dataset_path=dataset_path,
                feature_columns=regime_feature_columns,
                feature_set=regime_feature_set,
                model_version=regime_model_version,
                task=task,
                estimator_type=estimator_type,
                train_fraction=train_fraction,
                random_state=random_state,
                min_horizon_hours=min_horizon_hours,
                max_horizon_hours=max_horizon_hours,
                include_rolling_metrics=include_rolling_metrics,
                regime=regime,
                feature_selection=feature_selection,
            )
            regime_summary.update(
                {
                    "available": True,
                    "price_rule_accuracy": metrics.get("price_rule_accuracy"),
                    "price_saturated": metrics.get("price_saturated"),
                    "metrics": _compact_model_metrics(metrics, task=task),
                }
            )
        except RuntimeError as exc:
            regime_summary.update({"available": False, "reason": str(exc)})
        analysis[regime] = regime_summary
    return analysis


def _family_regime_analysis(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    train_fraction: float,
    random_state: int,
) -> dict[str, dict[str, Any]]:
    """Return compact model-family comparison by trade-covered and cold-start regimes."""
    analysis: dict[str, dict[str, Any]] = {}
    for regime in REGIME_ANALYSIS_ORDER:
        regime_rows = _filter_rows_by_regime(rows, regime)
        regime_feature_set, regime_feature_columns = _resolve_feature_columns(TASK_MARKET_OUTCOME, None, regime)
        regime_summary: dict[str, Any] = {
            **_regime_summary_context(regime),
            "row_count": len(regime_rows),
            "feature_set": regime_feature_set,
        }
        if not regime_rows:
            regime_summary.update({"available": False, "reason": "No rows matched this regime."})
            analysis[regime] = regime_summary
            continue
        try:
            _, random_forest_metrics, _ = _assess_market_model_rows(
                rows=regime_rows,
                dataset_path=dataset_path,
                feature_columns=regime_feature_columns,
                feature_set=regime_feature_set,
                model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "random_forest", regime_feature_set),
                task=TASK_MARKET_OUTCOME,
                estimator_type="random_forest",
                train_fraction=train_fraction,
                random_state=random_state,
                include_rolling_metrics=True,
                regime=regime,
            )
            _, lightgbm_metrics, _ = _assess_market_model_rows(
                rows=regime_rows,
                dataset_path=dataset_path,
                feature_columns=regime_feature_columns,
                feature_set=regime_feature_set,
                model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "lightgbm", regime_feature_set),
                task=TASK_MARKET_OUTCOME,
                estimator_type="lightgbm",
                train_fraction=train_fraction,
                random_state=random_state,
                include_rolling_metrics=True,
                regime=regime,
            )
            random_forest_rolling = _rolling_average_metrics(random_forest_metrics) or {}
            lightgbm_rolling = _rolling_average_metrics(lightgbm_metrics) or {}
            regime_summary.update(
                {
                    "available": True,
                    "price_rule_accuracy": lightgbm_metrics.get("price_rule_accuracy"),
                    "price_saturated": lightgbm_metrics.get("price_saturated"),
                    "random_forest": _compact_model_metrics(random_forest_metrics, task=TASK_MARKET_OUTCOME),
                    "lightgbm": _compact_model_metrics(lightgbm_metrics, task=TASK_MARKET_OUTCOME),
                    "lift": _lift_summary(random_forest_metrics, lightgbm_metrics, task=TASK_MARKET_OUTCOME),
                    "transition_gate": {
                        "primary_model": PRIMARY_ESTIMATOR_TYPE,
                        "random_forest_rolling": random_forest_rolling,
                        "lightgbm_rolling": lightgbm_rolling,
                        "random_forest_vs_price_rule": _relative_to_price_rule_summary(random_forest_metrics),
                        "lightgbm_vs_price_rule": _relative_to_price_rule_summary(lightgbm_metrics),
                        "lightgbm_ready": _transition_ready_from_rolling(
                            random_forest_rolling=random_forest_rolling,
                            lightgbm_rolling=lightgbm_rolling,
                        ),
                    },
                }
            )
        except RuntimeError as exc:
            regime_summary.update({"available": False, "reason": str(exc)})
        analysis[regime] = regime_summary
    return analysis


def _persist_training_artifacts(
    *,
    model: Any,
    metrics: dict[str, Any],
    feature_importances: list[tuple[str, float]],
    model_path: Path | None,
    metrics_path: Path | None,
    feature_importance_path: Path | None,
    report_path: Path | None,
) -> None:
    """Persist the training outputs that were requested by the caller."""
    if model_path is not None:
        model_path.parent.mkdir(parents=True, exist_ok=True)
        with model_path.open("wb") as handle:
            pickle.dump(model, handle)

    if metrics_path is not None:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with metrics_path.open("w", encoding="utf-8") as handle:
            json.dump(metrics, handle, indent=2, sort_keys=True)
            handle.write("\n")

    if feature_importance_path is not None:
        feature_importance_path.parent.mkdir(parents=True, exist_ok=True)
        with feature_importance_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["feature", "importance"])
            for feature, importance in feature_importances:
                writer.writerow([feature, round(float(importance), 10)])

    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(metrics, handle, indent=2, sort_keys=True)
            handle.write("\n")


def train_market_model(
    *,
    dataset_path: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    feature_importance_path: Path | None = None,
    report_path: Path | None = None,
    task: str = TASK_MARKET_OUTCOME,
    estimator_type: str = PRIMARY_ESTIMATOR_TYPE,
    feature_set: str | None = None,
    evaluation_mode: str = "single_split",
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> dict[str, Any]:
    """Train one market model and optionally attach rolling diagnostics."""
    task = _normalize_task(task)
    estimator_type = _normalize_estimator_type(estimator_type)
    regime = _normalize_regime(regime)
    feature_selection = _normalize_feature_selection(feature_selection)
    resolved_feature_set, feature_columns = _resolve_feature_columns(task, feature_set, regime)
    model_version = _resolve_model_version(task, estimator_type, resolved_feature_set)
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    include_rolling_metrics = str(evaluation_mode).strip().lower() == "rolling"
    if str(evaluation_mode).strip().lower() not in {"single_split", "rolling"}:
        raise RuntimeError(f"Unsupported evaluation_mode: {evaluation_mode}")
    filtered_rows = _filter_rows_by_regime(
        _filter_rows_by_horizon(
            _load_training_rows(dataset_path),
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
        ),
        regime,
    )

    model, metrics, feature_importances = _assess_market_model_rows(
        rows=filtered_rows,
        dataset_path=dataset_path,
        feature_columns=feature_columns,
        feature_set=resolved_feature_set,
        model_version=model_version,
        task=task,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        include_rolling_metrics=include_rolling_metrics,
        regime=regime,
        feature_selection=feature_selection,
    )
    metrics["evaluation_mode"] = "rolling" if include_rolling_metrics else "single_split"
    if regime == REGIME_ALL:
        metrics["regime_analysis"] = _regime_model_analysis(
            rows=filtered_rows,
            dataset_path=dataset_path,
            feature_columns=feature_columns,
            feature_set=resolved_feature_set,
            model_version=model_version,
            task=task,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            include_rolling_metrics=include_rolling_metrics,
            feature_selection=feature_selection,
        )
    _persist_training_artifacts(
        model=model,
        metrics=metrics,
        feature_importances=feature_importances,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=feature_importance_path,
        report_path=report_path,
    )
    return {
        "model_path": str(model_path) if model_path is not None else None,
        "metrics_path": str(metrics_path) if metrics_path is not None else None,
        "feature_importance_path": str(feature_importance_path) if feature_importance_path is not None else None,
        "report_path": str(report_path) if report_path is not None else None,
        "metrics": metrics,
    }


def _lift_summary(left: dict[str, Any], right: dict[str, Any], *, task: str) -> dict[str, float | None]:
    """Return directional lift metrics between two summaries."""
    if task == TASK_MARKET_OUTCOME:
        return {
            "accuracy_delta": round(float(right["accuracy"]) - float(left["accuracy"]), 6),
            "roc_auc_delta": (
                round(float(right["roc_auc"]) - float(left["roc_auc"]), 6)
                if left.get("roc_auc") is not None and right.get("roc_auc") is not None
                else None
            ),
            "f1_delta": round(float(right["f1"]) - float(left["f1"]), 6),
            "log_loss_delta": (
                round(float(right["log_loss"]) - float(left["log_loss"]), 6)
                if left.get("log_loss") is not None and right.get("log_loss") is not None
                else None
            ),
        }
    return {
        "mae_delta": round(float(right["mae"]) - float(left["mae"]), 6),
        "rmse_delta": round(float(right["rmse"]) - float(left["rmse"]), 6),
        "r2_delta": round(float(right["r2"]) - float(left["r2"]), 6),
    }


def _rolling_average_metrics(summary: dict[str, Any]) -> dict[str, Any] | None:
    """Return the average rolling metrics object when present."""
    rolling_metrics = summary.get("rolling_metrics")
    if not isinstance(rolling_metrics, dict):
        return None
    average = rolling_metrics.get("average")
    return average if isinstance(average, dict) else None


def _compact_model_metrics(summary: dict[str, Any], *, task: str) -> dict[str, Any]:
    """Return a compact model summary for banded or segmented reports."""
    compact: dict[str, Any] = {
        "feature_set": summary.get("feature_set"),
        "row_count": summary.get("row_count"),
        "train_rows": summary.get("train_rows"),
        "test_rows": summary.get("test_rows"),
        "train_condition_count": summary.get("train_condition_count"),
        "test_condition_count": summary.get("test_condition_count"),
        "price_rule_accuracy": summary.get("price_rule_accuracy"),
        "price_saturated": summary.get("price_saturated"),
        "coverage_segment_metrics": summary.get("coverage_segment_metrics"),
        "rolling_average": _rolling_average_metrics(summary),
    }
    if task == TASK_MARKET_OUTCOME:
        compact.update(
            {
                "accuracy": summary.get("accuracy"),
                "accuracy_vs_price_rule_delta": summary.get("accuracy_vs_price_rule_delta"),
                "f1": summary.get("f1"),
                "roc_auc": summary.get("roc_auc"),
                "log_loss": summary.get("log_loss"),
            }
        )
    else:
        compact.update(
            {
                "mae": summary.get("mae"),
                "rmse": summary.get("rmse"),
                "r2": summary.get("r2"),
            }
        )
    return compact


def _evaluate_whale_signal_feature_sets(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
) -> dict[str, dict[str, Any]]:
    """Evaluate the three fixed whale-signal feature sets on one filtered row slice."""
    feature_set_summaries: dict[str, dict[str, Any]] = {}
    for feature_set, feature_columns in (
        ("price_only", PRICE_BASELINE_FEATURE_COLUMNS),
        ("whale_only", WHALE_ONLY_FEATURE_COLUMNS),
        ("price_plus_whale", PRICE_PLUS_WHALE_FEATURE_COLUMNS),
    ):
        _, metrics, _ = _assess_market_model_rows(
            rows=rows,
            dataset_path=dataset_path,
            feature_columns=feature_columns,
            feature_set=feature_set,
            model_version=_resolve_model_version(TASK_WHALE_SIGNAL, estimator_type, feature_set),
            task=TASK_WHALE_SIGNAL,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            include_rolling_metrics=True,
            regime=regime,
        )
        feature_set_summaries[feature_set] = metrics
    return feature_set_summaries


def _evaluate_regression_feature_sets(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    task: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
    feature_selection: str = FEATURE_SELECTION_NONE,
) -> dict[str, dict[str, Any]]:
    """Evaluate price, whale, and combined feature sets for one regression task."""
    task = _normalize_task(task)
    if task not in REGRESSION_TASKS:
        raise RuntimeError(f"{task} is not a regression feature-set comparison task.")
    feature_selection = _normalize_feature_selection(feature_selection)

    feature_set_summaries: dict[str, dict[str, Any]] = {}
    for feature_set, feature_columns in (
        ("price_only", PRICE_BASELINE_FEATURE_COLUMNS),
        ("whale_only", WHALE_ONLY_FEATURE_COLUMNS),
        ("price_plus_whale", PRICE_PLUS_WHALE_FEATURE_COLUMNS),
    ):
        _, metrics, _ = _assess_market_model_rows(
            rows=rows,
            dataset_path=dataset_path,
            feature_columns=feature_columns,
            feature_set=feature_set,
            model_version=_resolve_model_version(task, estimator_type, feature_set),
            task=task,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            include_rolling_metrics=True,
            regime=regime,
            estimator_params=estimator_params,
            estimator_profile=estimator_profile,
            feature_selection=feature_selection,
        )
        feature_set_summaries[feature_set] = metrics
    return feature_set_summaries


def _summarize_whale_signal_feature_sets(
    feature_set_summaries: dict[str, dict[str, Any]],
    *,
    dataset_path: Path,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
) -> dict[str, Any]:
    """Build the whale-signal report summary from evaluated feature-set metrics."""
    price_only_metrics = feature_set_summaries["price_only"]
    whale_only_metrics = feature_set_summaries["whale_only"]
    price_plus_whale_metrics = feature_set_summaries["price_plus_whale"]
    price_saturated = bool(price_plus_whale_metrics["price_saturated"])
    price_plus_rolling = _rolling_average_metrics(price_plus_whale_metrics) or {}
    price_only_rolling = _rolling_average_metrics(price_only_metrics) or {}
    whale_lift_demonstrated = (
        price_plus_rolling.get("rmse") is not None
        and price_only_rolling.get("rmse") is not None
        and float(price_plus_rolling["rmse"]) < float(price_only_rolling["rmse"])
    )
    interpretation = (
        "Current data remains price-saturated; the residual whale-signal report runs end-to-end, "
        "but whale lift beyond price is not demonstrated on this export."
        if price_saturated and not whale_lift_demonstrated
        else "The residual whale-signal report quantifies whether whale features improve fit beyond price."
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        "task": TASK_WHALE_SIGNAL,
        "feature_set": "price_plus_whale",
        "row_count": price_plus_whale_metrics["row_count"],
        "train_rows": price_plus_whale_metrics["train_rows"],
        "test_rows": price_plus_whale_metrics["test_rows"],
        "train_condition_count": price_plus_whale_metrics["train_condition_count"],
        "test_condition_count": price_plus_whale_metrics["test_condition_count"],
        "train_fraction": train_fraction,
        "random_state": random_state,
        "min_horizon_hours": min_horizon_hours,
        "max_horizon_hours": max_horizon_hours,
        "price_rule_accuracy": price_plus_whale_metrics["price_rule_accuracy"],
        "price_saturated": price_saturated,
        "per_horizon_metrics": price_plus_whale_metrics["per_horizon_metrics"],
        "coverage_segment_metrics": price_plus_whale_metrics.get("coverage_segment_metrics"),
        "estimator_type": estimator_type,
        "assumptions": [
            "whale_signal uses resolution_edge = label_side_wins - price_baseline as the regression target.",
            "price_only is the reference residual model and price_plus_whale measures incremental whale lift beyond price.",
            "whale_only excludes direct price features and is diagnostic rather than a release gate.",
            "Whale predictive value must come from residual analysis, not from the compatibility market-outcome classifier alone.",
        ],
        "price_only": price_only_metrics,
        "whale_only": whale_only_metrics,
        "price_plus_whale": price_plus_whale_metrics,
        "lift_vs_price_only": _lift_summary(price_only_metrics, price_plus_whale_metrics, task=TASK_WHALE_SIGNAL),
        "lift_vs_whale_only": _lift_summary(whale_only_metrics, price_plus_whale_metrics, task=TASK_WHALE_SIGNAL),
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "interpretation": interpretation,
    }


def _horizon_band_whale_signal_analysis(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
) -> dict[str, dict[str, Any]]:
    """Return compact whale-signal analysis by horizon band."""
    analysis: dict[str, dict[str, Any]] = {}
    for band in HORIZON_BAND_DEFINITIONS:
        band_rows = _filter_rows_by_horizon(
            rows,
            min_horizon_hours=band["min_horizon_hours"],
            max_horizon_hours=band["max_horizon_hours"],
            max_horizon_hours_exclusive=band["max_horizon_hours_exclusive"],
        )
        band_summary: dict[str, Any] = {
            "label": band["label"],
            "min_horizon_hours": band["min_horizon_hours"],
            "max_horizon_hours": band["max_horizon_hours"],
            "max_horizon_hours_exclusive": band["max_horizon_hours_exclusive"],
            "row_count": len(band_rows),
        }
        if not band_rows:
            band_summary.update({"available": False, "reason": "No rows matched this horizon band."})
            analysis[band["name"]] = band_summary
            continue
        try:
            feature_set_summaries = _evaluate_whale_signal_feature_sets(
                rows=band_rows,
                dataset_path=dataset_path,
                estimator_type=estimator_type,
                train_fraction=train_fraction,
                random_state=random_state,
                min_horizon_hours=band["min_horizon_hours"],
                max_horizon_hours=band["max_horizon_hours"],
                regime=REGIME_ALL,
            )
            compact_price_only = _compact_model_metrics(feature_set_summaries["price_only"], task=TASK_WHALE_SIGNAL)
            compact_whale_only = _compact_model_metrics(feature_set_summaries["whale_only"], task=TASK_WHALE_SIGNAL)
            compact_price_plus_whale = _compact_model_metrics(
                feature_set_summaries["price_plus_whale"],
                task=TASK_WHALE_SIGNAL,
            )
            price_only_rolling = compact_price_only.get("rolling_average") or {}
            price_plus_rolling = compact_price_plus_whale.get("rolling_average") or {}
            whale_lift_demonstrated = (
                price_plus_rolling.get("rmse") is not None
                and price_only_rolling.get("rmse") is not None
                and float(price_plus_rolling["rmse"]) < float(price_only_rolling["rmse"])
            )
            band_summary.update(
                {
                    "available": True,
                    "price_rule_accuracy": feature_set_summaries["price_plus_whale"].get("price_rule_accuracy"),
                    "price_saturated": feature_set_summaries["price_plus_whale"].get("price_saturated"),
                    "price_only": compact_price_only,
                    "whale_only": compact_whale_only,
                    "price_plus_whale": compact_price_plus_whale,
                    "lift_vs_price_only": _lift_summary(
                        feature_set_summaries["price_only"],
                        feature_set_summaries["price_plus_whale"],
                        task=TASK_WHALE_SIGNAL,
                    ),
                    "whale_lift_demonstrated": whale_lift_demonstrated,
                }
            )
        except RuntimeError as exc:
            band_summary.update({"available": False, "reason": str(exc)})
        analysis[band["name"]] = band_summary
    return analysis


def _whale_signal_regime_analysis(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
) -> dict[str, dict[str, Any]]:
    """Return compact residual whale-signal analysis by trade-covered and cold-start regimes."""
    analysis: dict[str, dict[str, Any]] = {}
    for regime in REGIME_ANALYSIS_ORDER:
        regime_rows = _filter_rows_by_regime(rows, regime)
        regime_summary: dict[str, Any] = {
            **_regime_summary_context(regime),
            "row_count": len(regime_rows),
        }
        if not regime_rows:
            regime_summary.update({"available": False, "reason": "No rows matched this regime."})
            analysis[regime] = regime_summary
            continue
        try:
            feature_set_summaries = _evaluate_whale_signal_feature_sets(
                rows=regime_rows,
                dataset_path=dataset_path,
                estimator_type=estimator_type,
                train_fraction=train_fraction,
                random_state=random_state,
                min_horizon_hours=min_horizon_hours,
                max_horizon_hours=max_horizon_hours,
                regime=regime,
            )
            compact_price_only = _compact_model_metrics(feature_set_summaries["price_only"], task=TASK_WHALE_SIGNAL)
            compact_whale_only = _compact_model_metrics(feature_set_summaries["whale_only"], task=TASK_WHALE_SIGNAL)
            compact_price_plus_whale = _compact_model_metrics(
                feature_set_summaries["price_plus_whale"],
                task=TASK_WHALE_SIGNAL,
            )
            price_only_rolling = compact_price_only.get("rolling_average") or {}
            price_plus_rolling = compact_price_plus_whale.get("rolling_average") or {}
            whale_lift_demonstrated = (
                price_plus_rolling.get("rmse") is not None
                and price_only_rolling.get("rmse") is not None
                and float(price_plus_rolling["rmse"]) < float(price_only_rolling["rmse"])
            )
            interpretation = (
                "Cold-start rows remain a separate neutral problem; whale lift beyond price is not demonstrated in this regime."
                if regime == REGIME_COLD_START and not whale_lift_demonstrated
                else "Trade-covered rows remain price-determined, so whale lift beyond price is not demonstrated in this regime."
                if not whale_lift_demonstrated
                else "Whale lift beyond price is demonstrated in this regime."
            )
            regime_summary.update(
                {
                    "available": True,
                    "price_rule_accuracy": feature_set_summaries["price_plus_whale"].get("price_rule_accuracy"),
                    "price_saturated": feature_set_summaries["price_plus_whale"].get("price_saturated"),
                    "price_only": compact_price_only,
                    "whale_only": compact_whale_only,
                    "price_plus_whale": compact_price_plus_whale,
                    "lift_vs_price_only": _lift_summary(
                        feature_set_summaries["price_only"],
                        feature_set_summaries["price_plus_whale"],
                        task=TASK_WHALE_SIGNAL,
                    ),
                    "whale_lift_demonstrated": whale_lift_demonstrated,
                    "interpretation": interpretation,
                }
            )
        except RuntimeError as exc:
            regime_summary.update({"available": False, "reason": str(exc)})
        analysis[regime] = regime_summary
    return analysis


def _movement_task_for_window(window_hours: int) -> str:
    """Return the movement task name for a supported forward window."""
    if int(window_hours) == 12:
        return TASK_MARKET_MOVEMENT_12H
    if int(window_hours) == 24:
        return TASK_MARKET_MOVEMENT_24H
    raise RuntimeError(f"Unsupported movement window: {window_hours}")


def _movement_window_summary(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    window_hours: int,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None,
    max_horizon_hours: float | None,
    regime: str,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
) -> dict[str, Any]:
    """Return feature-set comparison summary for one movement window."""
    task = _movement_task_for_window(window_hours)
    feature_set_summaries = _evaluate_regression_feature_sets(
        rows=rows,
        dataset_path=dataset_path,
        task=task,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        regime=regime,
        estimator_params=estimator_params,
        estimator_profile=estimator_profile,
    )
    price_only_metrics = feature_set_summaries["price_only"]
    whale_only_metrics = feature_set_summaries["whale_only"]
    price_plus_whale_metrics = feature_set_summaries["price_plus_whale"]
    price_only_rolling = _rolling_average_metrics(price_only_metrics) or {}
    price_plus_rolling = _rolling_average_metrics(price_plus_whale_metrics) or {}
    whale_lift_demonstrated = (
        price_only_rolling.get("rmse") is not None
        and price_plus_rolling.get("rmse") is not None
        and float(price_plus_rolling["rmse"]) <= float(price_only_rolling["rmse"]) - MIN_ROLLING_RMSE_LIFT
    )
    interpretation = (
        f"Whale features materially improve {window_hours}h movement RMSE versus price-only on rolling evaluation."
        if whale_lift_demonstrated
        else f"Whale features do not materially improve {window_hours}h movement RMSE versus price-only on rolling evaluation."
    )

    return {
        "window_hours": window_hours,
        "task": task,
        "target_column": _target_column_for_task(task),
        "feature_set": "price_plus_whale",
        "estimator_profile": estimator_profile,
        "estimator_params": estimator_params or {},
        "row_count": price_plus_whale_metrics["row_count"],
        "train_rows": price_plus_whale_metrics["train_rows"],
        "test_rows": price_plus_whale_metrics["test_rows"],
        "train_condition_count": price_plus_whale_metrics["train_condition_count"],
        "test_condition_count": price_plus_whale_metrics["test_condition_count"],
        "price_rule_accuracy": price_plus_whale_metrics["price_rule_accuracy"],
        "price_saturated": price_plus_whale_metrics["price_saturated"],
        "per_horizon_metrics": price_plus_whale_metrics["per_horizon_metrics"],
        "coverage_segment_metrics": price_plus_whale_metrics.get("coverage_segment_metrics"),
        "price_only": price_only_metrics,
        "whale_only": whale_only_metrics,
        "price_plus_whale": price_plus_whale_metrics,
        "lift_vs_price_only": _lift_summary(price_only_metrics, price_plus_whale_metrics, task=task),
        "lift_vs_whale_only": _lift_summary(whale_only_metrics, price_plus_whale_metrics, task=task),
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "interpretation": interpretation,
    }


def compare_price_vs_whale_market_movement_models(
    *,
    dataset_path: Path | None = None,
    comparison_path: Path | None = None,
    estimator_type: str = PRIMARY_ESTIMATOR_TYPE,
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
    estimator_params: dict[str, Any] | None = None,
    estimator_profile: str = "default",
) -> dict[str, Any]:
    """Compare price-only vs whale-informed models for 12h and 24h market movement."""
    estimator_type = _normalize_estimator_type(estimator_type)
    regime = _normalize_regime(regime)
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_MOVEMENT_FEATURE_SET_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    all_rows = _filter_rows_by_horizon(
        _load_training_rows(dataset_path),
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
    )
    filtered_rows = _filter_rows_by_regime(all_rows, regime)
    if not filtered_rows:
        raise RuntimeError(f"No rows were found in {dataset_path} for movement comparison.")

    windows: dict[str, Any] = {}
    for window_hours in (12, 24):
        windows[f"{window_hours}h"] = _movement_window_summary(
            rows=filtered_rows,
            dataset_path=dataset_path,
            window_hours=window_hours,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            regime=regime,
            estimator_params=estimator_params,
            estimator_profile=estimator_profile,
        )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        **_regime_summary_context(regime),
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        "estimator_type": estimator_type,
        "estimator_profile": estimator_profile,
        "estimator_params": estimator_params or {},
        "task": "market_movement",
        "feature_set": "price_plus_whale",
        "train_fraction": train_fraction,
        "random_state": random_state,
        "min_horizon_hours": min_horizon_hours,
        "max_horizon_hours": max_horizon_hours,
        "row_count": len(filtered_rows),
        "assumptions": [
            "Movement targets measure future side-price deltas after the observation cutoff.",
            "Both windows compare price_only, whale_only, and price_plus_whale on the same grouped time split.",
            f"Whale lift is demonstrated only when price_plus_whale lowers rolling RMSE by at least {MIN_ROLLING_RMSE_LIFT}.",
            "This movement comparison is preferred over final-outcome accuracy for Week 10-11 whale entry/exit work.",
        ],
        "windows": windows,
        "overall_whale_lift_demonstrated": any(
            bool(window_summary.get("whale_lift_demonstrated"))
            for window_summary in windows.values()
        ),
    }

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "comparison_path": str(comparison_path),
        "summary": summary,
    }


def _selected_movement_tuning_profiles(
    *,
    profile_names: tuple[str, ...] | list[str] | None = None,
    estimator_types: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return the requested movement tuning profiles."""
    requested_profiles = {
        str(profile_name).strip().lower()
        for profile_name in (profile_names or [])
        if str(profile_name).strip()
    }
    requested_estimators = {
        _normalize_estimator_type(estimator_type)
        for estimator_type in (estimator_types or [])
        if str(estimator_type).strip()
    }

    selected: list[dict[str, Any]] = []
    matched_profiles: set[str] = set()
    for profile in MOVEMENT_TUNING_PROFILES:
        profile_name = str(profile["profile"]).strip().lower()
        estimator_type = _normalize_estimator_type(str(profile["estimator_type"]))
        if requested_profiles and profile_name not in requested_profiles:
            continue
        if requested_estimators and estimator_type not in requested_estimators:
            continue
        selected.append(
            {
                "profile": profile_name,
                "estimator_type": estimator_type,
                "description": str(profile.get("description", "")),
                "feature_selection": _normalize_feature_selection(
                    profile.get("feature_selection", FEATURE_SELECTION_NONE)
                ),
                "params": dict(profile.get("params", {})),
            }
        )
        matched_profiles.add(profile_name)

    missing_profiles = requested_profiles - matched_profiles
    if missing_profiles:
        raise RuntimeError(f"Unsupported movement tuning profile(s): {sorted(missing_profiles)}")
    if not selected:
        raise RuntimeError("No movement tuning profiles matched the requested filters.")
    return selected


def _round_or_none(value: Any) -> float | None:
    """Round a numeric value for compact reports."""
    return round(float(value), 6) if value is not None else None


def _metric_delta(right: Any, left: Any) -> float | None:
    """Return right-left when both values are present."""
    if right is None or left is None:
        return None
    return round(float(right) - float(left), 6)


def _regression_metrics(y_true: list[float], predictions: list[float]) -> dict[str, float]:
    """Return compact regression metrics."""
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    return {
        "mae": round(float(mean_absolute_error(y_true, predictions)), 6),
        "rmse": round(float(math.sqrt(mean_squared_error(y_true, predictions))), 6),
        "r2": round(float(r2_score(y_true, predictions)), 6),
    }


def _movement_lift_from_metrics(
    *,
    price_only_metrics: dict[str, Any],
    whale_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Return movement lift against price-only metrics."""
    rmse_delta = _metric_delta(whale_metrics.get("rmse"), price_only_metrics.get("rmse"))
    whale_lift_demonstrated = rmse_delta is not None and float(rmse_delta) <= -MIN_ROLLING_RMSE_LIFT
    return {
        "mae_delta": _metric_delta(whale_metrics.get("mae"), price_only_metrics.get("mae")),
        "rmse_delta": rmse_delta,
        "r2_delta": _metric_delta(whale_metrics.get("r2"), price_only_metrics.get("r2")),
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "minimum_required_rolling_rmse_delta": round(-MIN_ROLLING_RMSE_LIFT, 6),
    }


def _compact_feature_selection_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Return feature-selection diagnostics small enough for comparison reports."""
    selection = metrics.get("feature_selection") if isinstance(metrics.get("feature_selection"), dict) else {}
    rolling_metrics = metrics.get("rolling_metrics") if isinstance(metrics.get("rolling_metrics"), dict) else {}
    rolling_selection = (
        rolling_metrics.get("feature_selection")
        if isinstance(rolling_metrics.get("feature_selection"), dict)
        else {}
    )
    return {
        "mode": selection.get("mode", FEATURE_SELECTION_NONE),
        "requested_feature_count": selection.get("requested_feature_count"),
        "selected_feature_count": selection.get("selected_feature_count"),
        "candidate_whale_feature_count": selection.get("candidate_whale_feature_count"),
        "selected_whale_feature_count": selection.get("selected_whale_feature_count"),
        "dropped_whale_feature_count": selection.get("dropped_whale_feature_count"),
        "min_abs_correlation": selection.get("min_abs_correlation"),
        "max_selected_whale_features": selection.get("max_selected_whale_features"),
        "selected_whale_features": selection.get("selected_whale_features", [])[:8],
        "rolling": rolling_selection,
    }


def _compact_movement_tuning_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Return compact regression metrics for a movement tuning report."""
    rolling_average = _rolling_average_metrics(metrics) or {}
    rolling_metrics = metrics.get("rolling_metrics") if isinstance(metrics.get("rolling_metrics"), dict) else {}
    return {
        "feature_set": metrics.get("feature_set"),
        "row_count": metrics.get("row_count"),
        "train_rows": metrics.get("train_rows"),
        "test_rows": metrics.get("test_rows"),
        "train_condition_count": metrics.get("train_condition_count"),
        "test_condition_count": metrics.get("test_condition_count"),
        "single_split": {
            "mae": _round_or_none(metrics.get("mae")),
            "rmse": _round_or_none(metrics.get("rmse")),
            "r2": _round_or_none(metrics.get("r2")),
        },
        "rolling_average": {
            "fold_count": rolling_metrics.get("fold_count"),
            "mae": _round_or_none(rolling_average.get("mae")),
            "rmse": _round_or_none(rolling_average.get("rmse")),
            "r2": _round_or_none(rolling_average.get("r2")),
        },
        "generalization_gap": {
            "single_minus_rolling_mae": _metric_delta(metrics.get("mae"), rolling_average.get("mae")),
            "single_minus_rolling_rmse": _metric_delta(metrics.get("rmse"), rolling_average.get("rmse")),
        },
        "feature_selection": _compact_feature_selection_metrics(metrics),
        "top_features": metrics.get("top_features", [])[:8],
    }


def _movement_lift_gate(
    *,
    price_only_metrics: dict[str, Any],
    price_plus_whale_metrics: dict[str, Any],
    task: str,
) -> dict[str, Any]:
    """Return rolling-first whale lift and overperformance diagnostics."""
    single_lift = _lift_summary(price_only_metrics, price_plus_whale_metrics, task=task)
    price_only_rolling = _rolling_average_metrics(price_only_metrics) or {}
    price_plus_rolling = _rolling_average_metrics(price_plus_whale_metrics) or {}
    rolling_lift = {
        "mae_delta": _metric_delta(price_plus_rolling.get("mae"), price_only_rolling.get("mae")),
        "rmse_delta": _metric_delta(price_plus_rolling.get("rmse"), price_only_rolling.get("rmse")),
        "r2_delta": _metric_delta(price_plus_rolling.get("r2"), price_only_rolling.get("r2")),
    }
    rolling_rmse_delta = rolling_lift["rmse_delta"]
    single_rmse_delta = single_lift.get("rmse_delta")
    whale_lift_demonstrated = rolling_rmse_delta is not None and float(rolling_rmse_delta) <= -MIN_ROLLING_RMSE_LIFT
    single_split_only_lift = (
        single_rmse_delta is not None
        and float(single_rmse_delta) < 0
        and not whale_lift_demonstrated
    )
    return {
        "single_split_lift": single_lift,
        "rolling_lift": rolling_lift,
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "single_split_only_lift": single_split_only_lift,
        "passes_generalization_gate": whale_lift_demonstrated and not single_split_only_lift,
        "minimum_required_rolling_rmse_delta": round(-MIN_ROLLING_RMSE_LIFT, 6),
        "gate_rule": (
            "Accept whale lift only when price_plus_whale beats price_only on rolling RMSE "
            f"by at least {MIN_ROLLING_RMSE_LIFT}."
        ),
    }


def _movement_profile_window_report(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    window_hours: int,
    profile: dict[str, Any],
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None,
    max_horizon_hours: float | None,
    regime: str,
) -> dict[str, Any]:
    """Evaluate one tuning profile for one 12h/24h movement window."""
    task = _movement_task_for_window(window_hours)
    feature_set_summaries = _evaluate_regression_feature_sets(
        rows=rows,
        dataset_path=dataset_path,
        task=task,
        estimator_type=str(profile["estimator_type"]),
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        regime=regime,
        estimator_params=dict(profile.get("params", {})),
        estimator_profile=str(profile["profile"]),
        feature_selection=profile.get("feature_selection", FEATURE_SELECTION_NONE),
    )
    price_only_metrics = feature_set_summaries["price_only"]
    whale_only_metrics = feature_set_summaries["whale_only"]
    price_plus_whale_metrics = feature_set_summaries["price_plus_whale"]
    lift_gate = _movement_lift_gate(
        price_only_metrics=price_only_metrics,
        price_plus_whale_metrics=price_plus_whale_metrics,
        task=task,
    )
    return {
        "profile": profile["profile"],
        "estimator_type": profile["estimator_type"],
        "description": profile["description"],
        "estimator_params": profile["params"],
        "feature_selection": profile.get("feature_selection", FEATURE_SELECTION_NONE),
        "window_hours": window_hours,
        "task": task,
        "target_column": _target_column_for_task(task),
        "price_only": _compact_movement_tuning_metrics(price_only_metrics),
        "whale_only": _compact_movement_tuning_metrics(whale_only_metrics),
        "price_plus_whale": _compact_movement_tuning_metrics(price_plus_whale_metrics),
        "lift_vs_price_only": lift_gate,
    }


def _profile_rolling_rmse(profile_summary: dict[str, Any]) -> float | None:
    """Return the price-plus-whale rolling RMSE for one profile summary."""
    rmse = profile_summary.get("price_plus_whale", {}).get("rolling_average", {}).get("rmse")
    return float(rmse) if rmse is not None else None


def _profile_rolling_lift_rmse_delta(profile_summary: dict[str, Any]) -> float | None:
    """Return rolling price-plus-whale minus price-only RMSE delta."""
    delta = profile_summary.get("lift_vs_price_only", {}).get("rolling_lift", {}).get("rmse_delta")
    return float(delta) if delta is not None else None


def _best_movement_profile_summary(profile_summaries: dict[str, Any]) -> dict[str, Any]:
    """Select compact best-profile diagnostics for one movement window."""
    available_profiles = [
        summary
        for summary in profile_summaries.values()
        if summary.get("available", True) and _profile_rolling_rmse(summary) is not None
    ]
    if not available_profiles:
        return {
            "available": False,
            "reason": "No tuning profile produced rolling movement metrics.",
            "whale_lift_demonstrated": False,
        }

    best_rmse_profile = min(available_profiles, key=lambda item: _profile_rolling_rmse(item) or float("inf"))
    best_lift_profile = min(
        available_profiles,
        key=lambda item: (
            _profile_rolling_lift_rmse_delta(item)
            if _profile_rolling_lift_rmse_delta(item) is not None
            else float("inf")
        ),
    )
    best_lift_delta = _profile_rolling_lift_rmse_delta(best_lift_profile)
    whale_lift_demonstrated = best_lift_delta is not None and best_lift_delta <= -MIN_ROLLING_RMSE_LIFT
    return {
        "available": True,
        "best_price_plus_whale_profile": best_rmse_profile["profile"],
        "best_price_plus_whale_rolling_rmse": _profile_rolling_rmse(best_rmse_profile),
        "best_whale_lift_profile": best_lift_profile["profile"],
        "best_whale_lift_rolling_rmse_delta": best_lift_delta,
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "selected_profile": best_lift_profile["profile"] if whale_lift_demonstrated else None,
        "selection_reason": (
            "Selected because it materially improves rolling RMSE over price-only."
            if whale_lift_demonstrated
            else "No profile materially improved rolling RMSE over price-only; keep price-only as the benchmark."
        ),
    }


def _write_week10_11_market_movement_markdown(summary: dict[str, Any], markdown_path: Path) -> None:
    """Write a human-readable Week 10-11 movement report."""
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Week 10-11 Market Movement ML Report",
        "",
        f"Generated: {summary['generated_at']}",
        f"Dataset: `{summary['dataset_path']}`",
        f"Dataset version: `{summary['dataset_version']}`",
        f"Regime: `{summary['regime']}`",
        f"Rows evaluated: {summary['row_count']}",
        "",
        "## Decision",
        "",
        (
            "Whale lift is demonstrated on at least one rolling 12h/24h movement window."
            if summary["overall_whale_lift_demonstrated"]
            else "Whale lift is not yet demonstrated on rolling 12h/24h movement evaluation."
        ),
        "",
        "## Window Summary",
        "",
        "| Window | Best price+whale profile | Best whale-lift profile | Rolling RMSE delta | Lift shown |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for window_name, window_summary in summary["windows"].items():
        recommendation = window_summary.get("recommendation", {})
        lines.append(
            "| {window} | `{best_rmse}` | `{best_lift}` | {delta} | {lift} |".format(
                window=window_name,
                best_rmse=recommendation.get("best_price_plus_whale_profile"),
                best_lift=recommendation.get("best_whale_lift_profile"),
                delta=recommendation.get("best_whale_lift_rolling_rmse_delta"),
                lift="yes" if recommendation.get("whale_lift_demonstrated") else "no",
            )
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Tuning compares `price_only`, `whale_only`, and `price_plus_whale` for both 12h and 24h movement.",
            f"- Whale lift is accepted only when `price_plus_whale` lowers rolling RMSE versus `price_only` by at least {MIN_ROLLING_RMSE_LIFT}.",
            "- Single-split improvements are reported but do not count as proof of whale signal.",
            "- The final-outcome classifier remains a compatibility path, not the main Week 10-11 claim.",
            "",
            "## Implemented Criteria",
            "",
            "- Trusted whale scores are exposed through configurable weighted features.",
            "- Whale buy/sell frequency is represented in the movement feature set.",
            "- Entry, exit, holding-time, and realized-profit behavior are included as whale behavior features.",
            "- Recent trusted-whale entry/exit pressure is captured over 1h, 6h, 12h, and 24h pre-cutoff windows.",
            "- The tuning report prevents overclaiming when whale features only win on a single split.",
            "",
            f"JSON report: `{summary['report_path']}`",
        ]
    )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def tune_market_movement_models(
    *,
    dataset_path: Path | None = None,
    report_path: Path | None = None,
    markdown_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
    profile_names: tuple[str, ...] | list[str] | None = None,
    estimator_types: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    """Run a compact tuning report for 12h/24h whale movement models."""
    _require_ml_dependencies()
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    report_path = report_path or DEFAULT_MOVEMENT_TUNING_REPORT_PATH
    markdown_path = markdown_path or DEFAULT_WEEK10_11_MOVEMENT_REPORT_PATH
    regime = _normalize_regime(regime)
    profiles = _selected_movement_tuning_profiles(
        profile_names=profile_names,
        estimator_types=estimator_types,
    )
    filtered_rows = _filter_rows_by_regime(
        _filter_rows_by_horizon(
            _load_training_rows(dataset_path),
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
        ),
        regime,
    )
    if not filtered_rows:
        raise RuntimeError(f"No rows were found in {dataset_path} for movement tuning.")

    windows: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []
    for window_hours in (12, 24):
        profile_summaries: dict[str, Any] = {}
        for profile in profiles:
            profile_name = str(profile["profile"])
            try:
                profile_summaries[profile_name] = _movement_profile_window_report(
                    rows=filtered_rows,
                    dataset_path=dataset_path,
                    window_hours=window_hours,
                    profile=profile,
                    train_fraction=train_fraction,
                    random_state=random_state,
                    min_horizon_hours=min_horizon_hours,
                    max_horizon_hours=max_horizon_hours,
                    regime=regime,
                )
            except RuntimeError as exc:
                failure = {
                    "window_hours": window_hours,
                    "profile": profile_name,
                    "estimator_type": profile["estimator_type"],
                    "reason": str(exc),
                }
                profile_summaries[profile_name] = {"available": False, **failure}
                failures.append(failure)
        windows[f"{window_hours}h"] = {
            "window_hours": window_hours,
            "profiles": profile_summaries,
            "recommendation": _best_movement_profile_summary(profile_summaries),
        }

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "report_path": str(report_path),
        "markdown_path": str(markdown_path),
        **_regime_summary_context(regime),
        "task": "market_movement_tuning",
        "row_count": len(filtered_rows),
        "train_fraction": train_fraction,
        "random_state": random_state,
        "min_horizon_hours": min_horizon_hours,
        "max_horizon_hours": max_horizon_hours,
        "profiles": profiles,
        "windows": windows,
        "failures": failures,
        "overall_whale_lift_demonstrated": any(
            bool(window_summary.get("recommendation", {}).get("whale_lift_demonstrated"))
            for window_summary in windows.values()
        ),
        "assumptions": [
            "Forward movement uses side-price deltas at 12h and 24h after the observation cutoff.",
            "Tuning is judged by grouped rolling RMSE, not by single split performance.",
            f"Price-only remains the release baseline until whale features improve rolling RMSE by at least {MIN_ROLLING_RMSE_LIFT}.",
            "Profiles are intentionally compact so the report can be regenerated during Week 10-11 work.",
        ],
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
    _write_week10_11_market_movement_markdown(summary, markdown_path)

    return {
        "report_path": str(report_path),
        "markdown_path": str(markdown_path),
        "summary": summary,
    }


def _residual_estimator_params(estimator_type: str) -> dict[str, Any]:
    """Return the regularized estimator params used for residual experiments."""
    estimator_type = _normalize_estimator_type(estimator_type)
    if estimator_type == "random_forest":
        return dict(RESIDUAL_RANDOM_FOREST_PARAMS)
    return dict(RESIDUAL_LIGHTGBM_PARAMS)


def _predict_estimator(model: Any, features: list[list[float]]) -> list[float]:
    """Predict with warning filters for estimators that expect named features."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMRegressor was fitted with feature names",
            category=UserWarning,
        )
        return [float(value) for value in model.predict(features)]


def _residual_split_report(
    *,
    train_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
    task: str,
    estimator_type: str,
    random_state: int,
    min_abs_correlation: float,
    max_selected_whale_features: int,
) -> dict[str, Any]:
    """Fit price-only movement first, then fit whale features against the residual target."""
    task = _normalize_task(task)
    estimator_type = _normalize_estimator_type(estimator_type)
    estimator_params = _residual_estimator_params(estimator_type)
    y_train = _targets(train_rows, task)
    y_test = _targets(test_rows, task)

    price_model = _build_estimator(task, estimator_type, random_state, estimator_params=estimator_params)
    price_model.fit(_feature_matrix(train_rows, PRICE_BASELINE_FEATURE_COLUMNS), y_train)
    price_train_predictions = _predict_estimator(price_model, _feature_matrix(train_rows, PRICE_BASELINE_FEATURE_COLUMNS))
    price_test_predictions = _predict_estimator(price_model, _feature_matrix(test_rows, PRICE_BASELINE_FEATURE_COLUMNS))
    residual_train_targets = [
        float(target) - float(prediction)
        for target, prediction in zip(y_train, price_train_predictions, strict=True)
    ]

    residual_feature_columns, feature_selection = _training_correlation_feature_selection(
        train_rows=train_rows,
        feature_columns=WHALE_ONLY_FEATURE_COLUMNS,
        task=task,
        target_values=residual_train_targets,
        min_abs_correlation=min_abs_correlation,
        max_selected_whale_features=max_selected_whale_features,
    )
    residual_model = _build_estimator(task, estimator_type, random_state, estimator_params=estimator_params)
    residual_model.fit(_feature_matrix(train_rows, residual_feature_columns), residual_train_targets)
    residual_test_predictions = _predict_estimator(residual_model, _feature_matrix(test_rows, residual_feature_columns))
    corrected_predictions = [
        float(price_prediction) + float(residual_prediction)
        for price_prediction, residual_prediction in zip(price_test_predictions, residual_test_predictions, strict=True)
    ]
    price_metrics = _regression_metrics(y_test, price_test_predictions)
    corrected_metrics = _regression_metrics(y_test, corrected_predictions)
    return {
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "price_only": price_metrics,
        "residual_corrected": corrected_metrics,
        "lift_vs_price_only": _movement_lift_from_metrics(
            price_only_metrics=price_metrics,
            whale_metrics=corrected_metrics,
        ),
        "feature_selection": feature_selection,
        "selected_feature_columns": list(residual_feature_columns),
        "top_residual_features": [
            {"feature": feature, "importance": round(float(importance), 6)}
            for feature, importance in _feature_importance_rows(residual_model, residual_feature_columns)[:8]
        ],
    }


def _average_residual_fold_metrics(folds: list[dict[str, Any]], section: str) -> dict[str, Any]:
    """Average one metric section across residual rolling folds."""
    return {
        key: round(
            sum(float(fold[section][key]) for fold in folds) / len(folds),
            6,
        )
        for key in ("mae", "rmse", "r2")
    }


def _selection_stability_from_folds(folds: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize which whale features repeatedly survive selector sweeps."""
    fold_count = len(folds)
    feature_counts: dict[str, int] = {}
    correlation_totals: dict[str, float] = {}
    for fold in folds:
        selection = fold.get("feature_selection", {})
        selected_names = selection.get("selected_whale_feature_names")
        if not isinstance(selected_names, list):
            selected_names = [
                item.get("feature")
                for item in selection.get("selected_whale_features", [])
                if isinstance(item, dict) and item.get("feature")
            ]
        correlations_by_feature = {
            str(item.get("feature")): float(item.get("abs_correlation", 0.0))
            for item in selection.get("selected_whale_features", [])
            if isinstance(item, dict) and item.get("feature")
        }
        for feature in selected_names:
            feature_name = str(feature)
            feature_counts[feature_name] = feature_counts.get(feature_name, 0) + 1
            correlation_totals[feature_name] = correlation_totals.get(feature_name, 0.0) + correlations_by_feature.get(
                feature_name,
                0.0,
            )

    stable_threshold = max(2, math.ceil(fold_count * 0.5)) if fold_count else 0
    ranked = sorted(
        feature_counts,
        key=lambda feature: (
            -feature_counts[feature],
            -(correlation_totals.get(feature, 0.0) / max(feature_counts[feature], 1)),
            feature,
        ),
    )
    return {
        "fold_count": fold_count,
        "stable_threshold": stable_threshold,
        "distinct_selected_whale_feature_count": len(feature_counts),
        "stable_whale_feature_count": sum(1 for feature in feature_counts if feature_counts[feature] >= stable_threshold),
        "top_stable_whale_features": [
            {
                "feature": feature,
                "selected_fold_count": feature_counts[feature],
                "selection_frequency": round(feature_counts[feature] / fold_count, 6) if fold_count else 0.0,
                "average_abs_correlation": round(
                    correlation_totals.get(feature, 0.0) / max(feature_counts[feature], 1),
                    6,
                ),
            }
            for feature in ranked[:12]
        ],
    }


def _residual_rolling_report(
    *,
    rows: list[dict[str, Any]],
    task: str,
    estimator_type: str,
    random_state: int,
    min_abs_correlation: float,
    max_selected_whale_features: int,
) -> dict[str, Any]:
    """Run residual correction over grouped rolling folds."""
    split_definitions, configured_window_size, actual_window_size = _build_rolling_splits(rows)
    folds: list[dict[str, Any]] = []
    for split_definition in split_definitions:
        fold_report = _residual_split_report(
            train_rows=split_definition["train_rows"],
            test_rows=split_definition["test_rows"],
            task=task,
            estimator_type=estimator_type,
            random_state=random_state,
            min_abs_correlation=min_abs_correlation,
            max_selected_whale_features=max_selected_whale_features,
        )
        fold_report.update(
            {
                "fold_index": split_definition["fold_index"],
                "train_condition_count": len(split_definition["train_conditions"]),
                "test_condition_count": len(split_definition["test_conditions"]),
                "train_end_time_bucket_count": split_definition["train_bucket_count"],
                "test_end_time_bucket_count": split_definition["test_bucket_count"],
                "train_end_time_range": _split_time_range(split_definition["train_rows"]),
                "test_end_time_range": _split_time_range(split_definition["test_rows"]),
            }
        )
        folds.append(fold_report)

    price_average = _average_residual_fold_metrics(folds, "price_only")
    corrected_average = _average_residual_fold_metrics(folds, "residual_corrected")
    return {
        "split_unit": "market_end_time_bucket",
        "end_time_bucket_strategy": END_TIME_BUCKETING_STRATEGY,
        "configured_test_window_size": configured_window_size,
        "test_window_size": actual_window_size,
        "fold_count": len(folds),
        "price_only_average": price_average,
        "residual_corrected_average": corrected_average,
        "lift_vs_price_only": _movement_lift_from_metrics(
            price_only_metrics=price_average,
            whale_metrics=corrected_average,
        ),
        "feature_selection_stability": _selection_stability_from_folds(folds),
        "folds": folds,
    }


def _residual_config_report(
    *,
    rows: list[dict[str, Any]],
    task: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_abs_correlation: float,
    max_selected_whale_features: int,
) -> dict[str, Any]:
    """Evaluate one selector threshold/cap configuration."""
    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    single_split = _residual_split_report(
        train_rows=train_rows,
        test_rows=test_rows,
        task=task,
        estimator_type=estimator_type,
        random_state=random_state,
        min_abs_correlation=min_abs_correlation,
        max_selected_whale_features=max_selected_whale_features,
    )
    single_split.update(
        {
            "train_condition_count": len(train_conditions),
            "test_condition_count": len(test_conditions),
            "train_end_time_range": _split_time_range(train_rows),
            "test_end_time_range": _split_time_range(test_rows),
        }
    )
    rolling = _residual_rolling_report(
        rows=rows,
        task=task,
        estimator_type=estimator_type,
        random_state=random_state,
        min_abs_correlation=min_abs_correlation,
        max_selected_whale_features=max_selected_whale_features,
    )
    config_name = f"corr_{min_abs_correlation:g}_max_{max_selected_whale_features}"
    return {
        "config": config_name,
        "min_abs_correlation": min_abs_correlation,
        "max_selected_whale_features": max_selected_whale_features,
        "single_split": single_split,
        "rolling": rolling,
        "passes_generalization_gate": bool(rolling["lift_vs_price_only"].get("whale_lift_demonstrated")),
    }


def _best_residual_config(configs: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the best residual config by rolling RMSE delta."""
    if not configs:
        return {
            "available": False,
            "reason": "No residual configs were evaluated.",
            "whale_lift_demonstrated": False,
        }
    best = min(
        configs,
        key=lambda config: (
            float(config["rolling"]["lift_vs_price_only"].get("rmse_delta"))
            if config["rolling"]["lift_vs_price_only"].get("rmse_delta") is not None
            else float("inf")
        ),
    )
    best_delta = best["rolling"]["lift_vs_price_only"].get("rmse_delta")
    whale_lift_demonstrated = bool(best["rolling"]["lift_vs_price_only"].get("whale_lift_demonstrated"))
    return {
        "available": True,
        "selected_config": best["config"] if whale_lift_demonstrated else None,
        "best_config": best["config"],
        "best_rolling_rmse_delta": best_delta,
        "best_price_only_rolling_rmse": best["rolling"]["price_only_average"].get("rmse"),
        "best_residual_corrected_rolling_rmse": best["rolling"]["residual_corrected_average"].get("rmse"),
        "stable_whale_feature_count": best["rolling"]["feature_selection_stability"].get("stable_whale_feature_count"),
        "whale_lift_demonstrated": whale_lift_demonstrated,
        "selection_reason": (
            "Selected because residual whale correction materially improves rolling RMSE over price-only."
            if whale_lift_demonstrated
            else "No residual whale correction materially improved rolling RMSE over price-only."
        ),
    }


def _write_week10_11_residual_markdown(summary: dict[str, Any], markdown_path: Path) -> None:
    """Write a compact residual experiment table for Week 10-11 reporting."""
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Week 10-11 Residual Whale Movement Experiment",
        "",
        f"Generated: {summary['generated_at']}",
        f"Dataset: `{summary['dataset_path']}`",
        f"Regime: `{summary['regime']}`",
        f"Rows evaluated: {summary['row_count']}",
        "",
        "## Decision",
        "",
        (
            "Residual whale correction demonstrates rolling lift on at least one 12h/24h window."
            if summary["overall_residual_whale_lift_demonstrated"]
            else "Residual whale correction does not yet demonstrate rolling lift over price-only."
        ),
        "",
        "## Results",
        "",
        "| Window | Best config | Price-only RMSE | Residual RMSE | RMSE delta | Stable whale features | Lift shown |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for window_name, window_summary in summary["windows"].items():
        recommendation = window_summary.get("recommendation", {})
        lines.append(
            "| {window} | `{config}` | {price_rmse} | {residual_rmse} | {delta} | {stable_count} | {lift} |".format(
                window=window_name,
                config=recommendation.get("best_config"),
                price_rmse=recommendation.get("best_price_only_rolling_rmse"),
                residual_rmse=recommendation.get("best_residual_corrected_rolling_rmse"),
                delta=recommendation.get("best_rolling_rmse_delta"),
                stable_count=recommendation.get("stable_whale_feature_count"),
                lift="yes" if recommendation.get("whale_lift_demonstrated") else "no",
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Price-only is fitted first; the whale model predicts only the remaining movement residual.",
            "- Selector sweeps are fitted inside each training fold, so test folds do not influence selected whale columns.",
            f"- Lift requires rolling RMSE delta <= {-MIN_ROLLING_RMSE_LIFT}.",
            "- Current local data is limited by how many resolved markets have transaction history.",
            "",
            f"JSON report: `{summary['report_path']}`",
        ]
    )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def analyze_market_movement_residuals(
    *,
    dataset_path: Path | None = None,
    report_path: Path | None = None,
    markdown_path: Path | None = None,
    estimator_type: str = "random_forest",
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_TRADE_COVERED,
    selector_thresholds: tuple[float, ...] | list[float] | None = None,
    selector_max_features: tuple[int, ...] | list[int] | None = None,
) -> dict[str, Any]:
    """Analyze whether whale features can explain movement residuals left by price-only."""
    _require_ml_dependencies()
    estimator_type = _normalize_estimator_type(estimator_type)
    regime = _normalize_regime(regime)
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    report_path = report_path or DEFAULT_MOVEMENT_RESIDUAL_REPORT_PATH
    markdown_path = markdown_path or DEFAULT_WEEK10_11_RESIDUAL_REPORT_PATH
    thresholds = tuple(float(value) for value in (selector_thresholds or RESIDUAL_SELECTOR_THRESHOLDS))
    max_features_values = tuple(int(value) for value in (selector_max_features or RESIDUAL_SELECTOR_MAX_FEATURES))
    if not thresholds or any(value < 0 for value in thresholds):
        raise RuntimeError("selector_thresholds must contain non-negative values.")
    if not max_features_values or any(value <= 0 for value in max_features_values):
        raise RuntimeError("selector_max_features must contain positive values.")

    filtered_rows = _filter_rows_by_regime(
        _filter_rows_by_horizon(
            _load_training_rows(dataset_path),
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
        ),
        regime,
    )
    if not filtered_rows:
        raise RuntimeError(f"No rows were found in {dataset_path} for residual movement analysis.")

    windows: dict[str, Any] = {}
    for window_hours in (12, 24):
        task = _movement_task_for_window(window_hours)
        _require_valid_movement_targets(dataset_path=dataset_path, rows=filtered_rows, task=task)
        configs = [
            _residual_config_report(
                rows=filtered_rows,
                task=task,
                estimator_type=estimator_type,
                train_fraction=train_fraction,
                random_state=random_state,
                min_abs_correlation=threshold,
                max_selected_whale_features=max_features,
            )
            for threshold in thresholds
            for max_features in max_features_values
        ]
        windows[f"{window_hours}h"] = {
            "window_hours": window_hours,
            "task": task,
            "target_column": _target_column_for_task(task),
            "configs": {config["config"]: config for config in configs},
            "recommendation": _best_residual_config(configs),
        }

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "report_path": str(report_path),
        "markdown_path": str(markdown_path),
        **_regime_summary_context(regime),
        "task": "market_movement_residuals",
        "row_count": len(filtered_rows),
        "estimator_type": estimator_type,
        "estimator_params": _residual_estimator_params(estimator_type),
        "train_fraction": train_fraction,
        "random_state": random_state,
        "min_horizon_hours": min_horizon_hours,
        "max_horizon_hours": max_horizon_hours,
        "selector_thresholds": list(thresholds),
        "selector_max_features": list(max_features_values),
        "minimum_required_rolling_rmse_delta": round(-MIN_ROLLING_RMSE_LIFT, 6),
        "windows": windows,
        "overall_residual_whale_lift_demonstrated": any(
            bool(window_summary.get("recommendation", {}).get("whale_lift_demonstrated"))
            for window_summary in windows.values()
        ),
        "assumptions": [
            "Price-only movement is fitted first on each training fold.",
            "The residual model predicts only movement left unexplained by price-only.",
            "Whale feature selection is fitted inside each training fold using residual-target correlation.",
            "Selector stability counts whale features that survive across rolling folds.",
        ],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
    _write_week10_11_residual_markdown(summary, markdown_path)
    return {
        "report_path": str(report_path),
        "markdown_path": str(markdown_path),
        "summary": summary,
    }


def _whale_feature_groups() -> dict[str, tuple[str, ...]]:
    """Return whale feature groups used by sparsity and ablation reports."""
    recent = tuple(column for column in RECENT_TRUSTED_WHALE_FEATURE_COLUMNS if column in WHALE_FEATURE_COLUMNS)
    behavior_names = {
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
    }
    weighted_pressure = tuple(
        column
        for column in WHALE_FEATURE_COLUMNS
        if "weighted" in column and column not in recent
    )
    scored_whale_weighted_pressure = tuple(
        column for column in weighted_pressure if column.startswith("whale_")
    )
    trusted_whale_weighted_pressure = tuple(
        column for column in weighted_pressure if column.startswith("trusted_whale_")
    )
    frequency = tuple(
        column
        for column in WHALE_FEATURE_COLUMNS
        if (
            "trade_count" in column
            or "trade_share" in column
            or "distinct_users" in column
            or "buy_sell_ratio" in column
            or "trades_per_active_day" in column
        )
        and column not in recent
    )
    notional = tuple(
        column
        for column in WHALE_FEATURE_COLUMNS
        if (
            "notional" in column
            or column in {"whale_vs_crowd_side_net_notional_gap", "trusted_whale_vs_crowd_side_net_notional_gap"}
        )
        and column not in recent
    )
    timing = tuple(column for column in WHALE_FEATURE_COLUMNS if column.endswith("_trade_age_side_hours"))
    behavior = tuple(column for column in WHALE_FEATURE_COLUMNS if column in behavior_names)
    grouped = set(recent + weighted_pressure + frequency + notional + timing + behavior)
    other = tuple(column for column in WHALE_FEATURE_COLUMNS if column not in grouped)
    return {
        "recent_pressure": recent,
        "weighted_pressure": weighted_pressure,
        "scored_whale_weighted_pressure": scored_whale_weighted_pressure,
        "trusted_whale_weighted_pressure": trusted_whale_weighted_pressure,
        "frequency": frequency,
        "notional": notional,
        "timing": timing,
        "position_behavior": behavior,
        "other_whale": other,
        "all_whale": WHALE_FEATURE_COLUMNS,
    }


def _feature_sparsity_stats(rows: list[dict[str, Any]], feature_columns: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """Return per-feature density and variation diagnostics."""
    stats: dict[str, dict[str, Any]] = {}
    row_count = len(rows)
    for feature in feature_columns:
        values = [float(row.get(feature, 0.0) or 0.0) for row in rows]
        nonzero_count = sum(1 for value in values if abs(value) > 1e-12)
        unique_values = {round(value, 10) for value in values}
        mean_value = sum(values) / row_count if row_count else 0.0
        mean_abs_value = sum(abs(value) for value in values) / row_count if row_count else 0.0
        variance = (
            sum((value - mean_value) ** 2 for value in values) / row_count
            if row_count
            else 0.0
        )
        stats[feature] = {
            "row_count": row_count,
            "nonzero_count": nonzero_count,
            "nonzero_fraction": round(nonzero_count / row_count, 6) if row_count else 0.0,
            "zero_fraction": round(1.0 - (nonzero_count / row_count), 6) if row_count else 1.0,
            "unique_count": len(unique_values),
            "is_constant": len(unique_values) <= 1,
            "mean": round(mean_value, 8),
            "mean_abs": round(mean_abs_value, 8),
            "stddev": round(math.sqrt(variance), 8),
            "min": round(min(values), 8) if values else 0.0,
            "max": round(max(values), 8) if values else 0.0,
        }
    return stats


def _feature_group_sparsity_summary(
    feature_stats: dict[str, dict[str, Any]],
    feature_groups: dict[str, tuple[str, ...]],
) -> dict[str, dict[str, Any]]:
    """Return compact density summaries for whale feature groups."""
    summaries: dict[str, dict[str, Any]] = {}
    for group_name, columns in feature_groups.items():
        stats = [feature_stats[column] for column in columns if column in feature_stats]
        if not stats:
            summaries[group_name] = {
                "feature_count": 0,
                "nonzero_feature_count": 0,
                "constant_feature_count": 0,
                "average_nonzero_fraction": 0.0,
                "max_nonzero_fraction": 0.0,
                "sparsest_features": [],
            }
            continue
        summaries[group_name] = {
            "feature_count": len(stats),
            "nonzero_feature_count": sum(1 for item in stats if int(item["nonzero_count"]) > 0),
            "constant_feature_count": sum(1 for item in stats if bool(item["is_constant"])),
            "average_nonzero_fraction": round(
                sum(float(item["nonzero_fraction"]) for item in stats) / len(stats),
                6,
            ),
            "max_nonzero_fraction": round(max(float(item["nonzero_fraction"]) for item in stats), 6),
            "sparsest_features": [
                {
                    "feature": feature,
                    "nonzero_fraction": feature_stats[feature]["nonzero_fraction"],
                    "unique_count": feature_stats[feature]["unique_count"],
                }
                for feature in sorted(
                    columns,
                    key=lambda column: (
                        float(feature_stats.get(column, {}).get("nonzero_fraction", 1.0)),
                        int(feature_stats.get(column, {}).get("unique_count", 0)),
                    ),
                )[:8]
                if feature in feature_stats
            ],
        }
    return summaries


def _movement_ablation_feature_sets() -> dict[str, tuple[str, ...]]:
    """Return price-plus-whale ablation feature sets for movement diagnostics."""
    groups = _whale_feature_groups()
    recent = set(groups["recent_pressure"])
    default_non_recent = tuple(column for column in DEFAULT_WHALE_FEATURE_COLUMNS if column not in recent)
    all_non_recent = tuple(column for column in WHALE_FEATURE_COLUMNS if column not in recent)
    return {
        "price_only": PRICE_BASELINE_FEATURE_COLUMNS,
        "price_plus_all_whale": PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        "price_plus_selected_whale": PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        "price_plus_all_whale_with_scored_pressure": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + WHALE_FEATURE_COLUMNS
        ),
        "price_plus_selected_whale_with_scored_pressure": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + WHALE_FEATURE_COLUMNS
        ),
        "price_plus_without_recent_whale": _dedupe_columns(PRICE_BASELINE_FEATURE_COLUMNS + default_non_recent),
        "price_plus_without_recent_whale_with_scored_pressure": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + all_non_recent
        ),
        "price_plus_recent_whale_only": _dedupe_columns(PRICE_BASELINE_FEATURE_COLUMNS + groups["recent_pressure"]),
        "price_plus_weighted_pressure_only": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + groups["weighted_pressure"]
        ),
        "price_plus_scored_whale_weighted_pressure_only": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + groups["scored_whale_weighted_pressure"]
        ),
        "price_plus_trusted_whale_weighted_pressure_only": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + groups["trusted_whale_weighted_pressure"]
        ),
        "price_plus_position_behavior_only": _dedupe_columns(
            PRICE_BASELINE_FEATURE_COLUMNS + groups["position_behavior"]
        ),
        "price_plus_timing_only": _dedupe_columns(PRICE_BASELINE_FEATURE_COLUMNS + groups["timing"]),
    }


def _movement_ablation_feature_selection(feature_set: str) -> str:
    """Return feature-selection mode for one movement ablation feature set."""
    if str(feature_set).strip().lower() in {
        "price_plus_selected_whale",
        "price_plus_selected_whale_with_scored_pressure",
    }:
        return FEATURE_SELECTION_TRAINING_CORRELATION
    return FEATURE_SELECTION_NONE


def _movement_ablation_window_report(
    *,
    rows: list[dict[str, Any]],
    dataset_path: Path,
    window_hours: int,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
    min_horizon_hours: float | None,
    max_horizon_hours: float | None,
    regime: str,
) -> dict[str, Any]:
    """Evaluate whale feature ablations for one movement window."""
    task = _movement_task_for_window(window_hours)
    feature_sets = _movement_ablation_feature_sets()
    metrics_by_set: dict[str, dict[str, Any]] = {}
    for feature_set, feature_columns in feature_sets.items():
        _, metrics, _ = _assess_market_model_rows(
            rows=rows,
            dataset_path=dataset_path,
            feature_columns=feature_columns,
            feature_set=feature_set,
            model_version=f"{task}_{feature_set}_{estimator_type}_ablation_v1",
            task=task,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            include_rolling_metrics=True,
            regime=regime,
            feature_selection=_movement_ablation_feature_selection(feature_set),
        )
        metrics_by_set[feature_set] = metrics

    price_only_metrics = metrics_by_set["price_only"]
    compact_feature_sets = {
        feature_set: _compact_movement_tuning_metrics(metrics)
        for feature_set, metrics in metrics_by_set.items()
    }
    lift_vs_price_only = {
        feature_set: _movement_lift_gate(
            price_only_metrics=price_only_metrics,
            price_plus_whale_metrics=metrics,
            task=task,
        )
        for feature_set, metrics in metrics_by_set.items()
        if feature_set != "price_only"
    }
    material_lift_sets = [
        feature_set
        for feature_set, lift_summary in lift_vs_price_only.items()
        if bool(lift_summary.get("whale_lift_demonstrated"))
    ]
    best_lift_set = min(
        lift_vs_price_only,
        key=lambda feature_set: (
            float(lift_vs_price_only[feature_set].get("rolling_lift", {}).get("rmse_delta"))
            if lift_vs_price_only[feature_set].get("rolling_lift", {}).get("rmse_delta") is not None
            else float("inf")
        ),
        default=None,
    )
    return {
        "window_hours": window_hours,
        "task": task,
        "target_column": _target_column_for_task(task),
        "feature_sets": compact_feature_sets,
        "lift_vs_price_only": lift_vs_price_only,
        "best_lift_feature_set": best_lift_set,
        "best_lift_rolling_rmse_delta": (
            lift_vs_price_only[best_lift_set]["rolling_lift"]["rmse_delta"]
            if best_lift_set is not None
            else None
        ),
        "material_lift_feature_sets": material_lift_sets,
        "material_whale_lift_demonstrated": bool(material_lift_sets),
    }


def analyze_whale_feature_ablation(
    *,
    dataset_path: Path | None = None,
    report_path: Path | None = None,
    estimator_type: str = "random_forest",
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
) -> dict[str, Any]:
    """Analyze whale feature sparsity and movement-model ablations."""
    _require_ml_dependencies()
    estimator_type = _normalize_estimator_type(estimator_type)
    regime = _normalize_regime(regime)
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    report_path = report_path or DEFAULT_WHALE_FEATURE_ABLATION_REPORT_PATH
    report_path.parent.mkdir(parents=True, exist_ok=True)

    filtered_rows = _filter_rows_by_regime(
        _filter_rows_by_horizon(
            _load_training_rows(dataset_path),
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
        ),
        regime,
    )
    if not filtered_rows:
        raise RuntimeError(f"No rows were found in {dataset_path} for whale feature ablation.")

    feature_groups = _whale_feature_groups()
    feature_stats = _feature_sparsity_stats(filtered_rows, WHALE_FEATURE_COLUMNS)
    group_summaries = _feature_group_sparsity_summary(feature_stats, feature_groups)
    sparse_features = [
        {"feature": feature, **stats}
        for feature, stats in sorted(
            feature_stats.items(),
            key=lambda item: (float(item[1]["nonzero_fraction"]), int(item[1]["unique_count"])),
        )
        if float(stats["nonzero_fraction"]) <= 0.05
    ][:25]
    dense_features = [
        {"feature": feature, **stats}
        for feature, stats in sorted(
            feature_stats.items(),
            key=lambda item: float(item[1]["nonzero_fraction"]),
            reverse=True,
        )
    ][:25]

    windows = {
        f"{window_hours}h": _movement_ablation_window_report(
            rows=filtered_rows,
            dataset_path=dataset_path,
            window_hours=window_hours,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
            regime=regime,
        )
        for window_hours in (12, 24)
    }
    material_lift_windows = [
        window_name
        for window_name, window_summary in windows.items()
        if bool(window_summary.get("material_whale_lift_demonstrated"))
    ]
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "report_path": str(report_path),
        **_regime_summary_context(regime),
        "task": "whale_feature_ablation",
        "row_count": len(filtered_rows),
        "estimator_type": estimator_type,
        "train_fraction": train_fraction,
        "random_state": random_state,
        "min_horizon_hours": min_horizon_hours,
        "max_horizon_hours": max_horizon_hours,
        "minimum_required_rolling_rmse_delta": round(-MIN_ROLLING_RMSE_LIFT, 6),
        "feature_group_columns": {name: list(columns) for name, columns in feature_groups.items()},
        "feature_group_summaries": group_summaries,
        "sparse_features": sparse_features,
        "dense_features": dense_features,
        "windows": windows,
        "material_lift_windows": material_lift_windows,
        "overall_material_whale_lift_demonstrated": bool(material_lift_windows),
        "interpretation": (
            "At least one whale feature ablation materially improves rolling RMSE beyond price-only."
            if material_lift_windows
            else "No whale feature group materially improves rolling RMSE beyond price-only; inspect sparse and dense feature groups before adding more model complexity."
        ),
        "assumptions": [
            "Ablations reuse grouped market_end_time rolling splits.",
            "price_only is the release baseline; each whale group is judged by rolling RMSE delta versus that baseline.",
            f"Material lift requires rolling RMSE delta <= {-MIN_ROLLING_RMSE_LIFT}.",
            "Sparse feature diagnostics treat exact zero as absence of signal for numeric whale features.",
        ],
    }

    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "report_path": str(report_path),
        "summary": summary,
    }


def compare_price_vs_whale_market_models(
    *,
    dataset_path: Path | None = None,
    comparison_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
    regime: str = REGIME_ALL,
) -> dict[str, Any]:
    """Compare random-forest price-only and price-plus-whale models on the same grouped split."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    regime = _normalize_regime(regime)

    _, price_metrics, _ = _assess_market_model(
        dataset_path=dataset_path,
        feature_columns=PRICE_BASELINE_FEATURE_COLUMNS,
        feature_set="price_only",
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "random_forest", "price_only"),
        task=TASK_MARKET_OUTCOME,
        estimator_type="random_forest",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )
    _, whale_metrics, _ = _assess_market_model(
        dataset_path=dataset_path,
        feature_columns=PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        feature_set="price_plus_whale",
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "random_forest", "price_plus_whale"),
        task=TASK_MARKET_OUTCOME,
        estimator_type="random_forest",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        **_regime_summary_context(regime),
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        "task": TASK_MARKET_OUTCOME,
        "feature_set": "price_plus_whale",
        "row_count": price_metrics["row_count"],
        "train_rows": price_metrics["train_rows"],
        "test_rows": price_metrics["test_rows"],
        "train_condition_count": price_metrics["train_condition_count"],
        "test_condition_count": price_metrics["test_condition_count"],
        "train_fraction": train_fraction,
        "random_state": random_state,
        "price_rule_accuracy": price_metrics["price_rule_accuracy"],
        "price_saturated": price_metrics["price_saturated"],
        "per_horizon_metrics": price_metrics["per_horizon_metrics"],
        "price_rule_baseline": _relative_to_price_rule_summary(price_metrics),
        "assumptions": [
            "Both models use the same grouped time split by market_end_time bucket.",
            "The price-plus-whale model adds whale participation features to the same price/context baseline.",
            "Whale participation features are computed from trade and resolved-market history available on or before each observation cutoff.",
            "Historical whale exposure uses an open-share proxy valued at average buy price.",
        ],
        "price_only": price_metrics,
        "price_plus_whale": whale_metrics,
        "lift": _lift_summary(price_metrics, whale_metrics, task=TASK_MARKET_OUTCOME),
    }

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "comparison_path": str(comparison_path),
        "summary": summary,
    }


def compare_price_vs_whale_market_models_lightgbm(
    *,
    dataset_path: Path | None = None,
    comparison_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
    regime: str = REGIME_ALL,
) -> dict[str, Any]:
    """Compare LightGBM price-only and price-plus-whale models on the same grouped split."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_LIGHTGBM_FEATURE_SET_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    regime = _normalize_regime(regime)

    _, price_metrics, _ = _assess_market_model(
        dataset_path=dataset_path,
        feature_columns=PRICE_BASELINE_FEATURE_COLUMNS,
        feature_set="price_only",
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "lightgbm", "price_only"),
        task=TASK_MARKET_OUTCOME,
        estimator_type="lightgbm",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )
    _, whale_metrics, _ = _assess_market_model(
        dataset_path=dataset_path,
        feature_columns=PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        feature_set="price_plus_whale",
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "lightgbm", "price_plus_whale"),
        task=TASK_MARKET_OUTCOME,
        estimator_type="lightgbm",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        **_regime_summary_context(regime),
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        "task": TASK_MARKET_OUTCOME,
        "feature_set": "price_plus_whale",
        "row_count": price_metrics["row_count"],
        "train_rows": price_metrics["train_rows"],
        "test_rows": price_metrics["test_rows"],
        "train_condition_count": price_metrics["train_condition_count"],
        "test_condition_count": price_metrics["test_condition_count"],
        "train_fraction": train_fraction,
        "random_state": random_state,
        "price_rule_accuracy": price_metrics["price_rule_accuracy"],
        "price_saturated": price_metrics["price_saturated"],
        "per_horizon_metrics": price_metrics["per_horizon_metrics"],
        "price_rule_baseline": _relative_to_price_rule_summary(price_metrics),
        "assumptions": [
            "Both LightGBM models use the same grouped time split by market_end_time bucket.",
            "The price-plus-whale model adds whale participation features to the same price/context baseline.",
            "Whale participation features are computed from trade and resolved-market history available on or before each observation cutoff.",
            "Historical whale exposure uses an open-share proxy valued at average buy price.",
        ],
        "price_only": price_metrics,
        "price_plus_whale": whale_metrics,
        "lift": _lift_summary(price_metrics, whale_metrics, task=TASK_MARKET_OUTCOME),
    }

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "comparison_path": str(comparison_path),
        "summary": summary,
    }


def compare_market_model_families(
    *,
    dataset_path: Path | None = None,
    comparison_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
    regime: str = REGIME_ALL,
) -> dict[str, Any]:
    """Compare Random Forest and LightGBM on the same grouped time split and feature set."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_MODEL_FAMILY_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    regime = _normalize_regime(regime)
    all_rows = _load_training_rows(dataset_path)
    filtered_rows = _filter_rows_by_regime(all_rows, regime)
    resolved_feature_set, feature_columns = _resolve_feature_columns(TASK_MARKET_OUTCOME, None, regime)

    _, random_forest_metrics, _ = _assess_market_model_rows(
        rows=filtered_rows,
        dataset_path=dataset_path,
        feature_columns=feature_columns,
        feature_set=resolved_feature_set,
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "random_forest", resolved_feature_set),
        task=TASK_MARKET_OUTCOME,
        estimator_type="random_forest",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )
    _, lightgbm_metrics, _ = _assess_market_model_rows(
        rows=filtered_rows,
        dataset_path=dataset_path,
        feature_columns=feature_columns,
        feature_set=resolved_feature_set,
        model_version=_resolve_model_version(TASK_MARKET_OUTCOME, "lightgbm", resolved_feature_set),
        task=TASK_MARKET_OUTCOME,
        estimator_type="lightgbm",
        train_fraction=train_fraction,
        random_state=random_state,
        include_rolling_metrics=True,
        regime=regime,
    )

    random_forest_rolling = _rolling_average_metrics(random_forest_metrics) or {}
    lightgbm_rolling = _rolling_average_metrics(lightgbm_metrics) or {}
    transition_ready = _transition_ready_from_rolling(
        random_forest_rolling=random_forest_rolling,
        lightgbm_rolling=lightgbm_rolling,
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        **_regime_summary_context(regime),
        "primary_model": PRIMARY_ESTIMATOR_TYPE,
        "task": TASK_MARKET_OUTCOME,
        "feature_set": resolved_feature_set,
        "row_count": lightgbm_metrics["row_count"],
        "train_rows": lightgbm_metrics["train_rows"],
        "test_rows": lightgbm_metrics["test_rows"],
        "train_condition_count": lightgbm_metrics["train_condition_count"],
        "test_condition_count": lightgbm_metrics["test_condition_count"],
        "train_fraction": train_fraction,
        "random_state": random_state,
        "price_rule_accuracy": lightgbm_metrics["price_rule_accuracy"],
        "price_saturated": lightgbm_metrics["price_saturated"],
        "per_horizon_metrics": lightgbm_metrics["per_horizon_metrics"],
        "lightgbm_vs_price_rule": _relative_to_price_rule_summary(lightgbm_metrics),
        "random_forest_vs_price_rule": _relative_to_price_rule_summary(random_forest_metrics),
        "assumptions": [
            "Both models use the same grouped time split by market_end_time bucket.",
            "Both models use the same full market feature set from the current dataset version.",
            "This comparison is for model-family selection only; feature engineering remains the larger bottleneck.",
            "LightGBM is the declared primary model family and Random Forest is benchmark-only.",
        ],
        "random_forest": random_forest_metrics,
        "lightgbm": lightgbm_metrics,
        "lift": _lift_summary(random_forest_metrics, lightgbm_metrics, task=TASK_MARKET_OUTCOME),
        "transition_gate": {
            "primary_model": PRIMARY_ESTIMATOR_TYPE,
            "random_forest_rolling": random_forest_rolling,
            "lightgbm_rolling": lightgbm_rolling,
            "random_forest_vs_price_rule": _relative_to_price_rule_summary(random_forest_metrics),
            "lightgbm_vs_price_rule": _relative_to_price_rule_summary(lightgbm_metrics),
            "lightgbm_ready": transition_ready,
        },
    }
    if regime == REGIME_ALL:
        summary["regime_analysis"] = _family_regime_analysis(
            rows=all_rows,
            dataset_path=dataset_path,
            train_fraction=train_fraction,
            random_state=random_state,
        )

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "comparison_path": str(comparison_path),
        "summary": summary,
    }


def analyze_market_whale_signal(
    *,
    dataset_path: Path | None = None,
    analysis_path: Path | None = None,
    estimator_type: str = PRIMARY_ESTIMATOR_TYPE,
    train_fraction: float = 0.75,
    random_state: int = 42,
    min_horizon_hours: float | None = None,
    max_horizon_hours: float | None = None,
    regime: str = REGIME_ALL,
) -> dict[str, Any]:
    """Run the residual-based whale-signal analysis across the fixed feature sets."""
    estimator_type = _normalize_estimator_type(estimator_type)
    regime = _normalize_regime(regime)
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    analysis_path = analysis_path or DEFAULT_WHALE_SIGNAL_ANALYSIS_PATH
    analysis_path.parent.mkdir(parents=True, exist_ok=True)
    all_rows = _filter_rows_by_horizon(
        _load_training_rows(dataset_path),
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
    )
    filtered_rows = _filter_rows_by_regime(all_rows, regime)
    feature_set_summaries = _evaluate_whale_signal_feature_sets(
        rows=filtered_rows,
        dataset_path=dataset_path,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        regime=regime,
    )
    summary = _summarize_whale_signal_feature_sets(
        feature_set_summaries,
        dataset_path=dataset_path,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
    )
    summary.update(_regime_summary_context(regime))
    summary["horizon_band_analysis"] = _horizon_band_whale_signal_analysis(
        rows=filtered_rows,
        dataset_path=dataset_path,
        estimator_type=estimator_type,
        train_fraction=train_fraction,
        random_state=random_state,
    )
    overall_whale_lift_demonstrated = bool(summary["whale_lift_demonstrated"])
    summary["overall_whale_lift_demonstrated"] = overall_whale_lift_demonstrated
    if regime == REGIME_ALL:
        summary["regime_analysis"] = _whale_signal_regime_analysis(
            rows=all_rows,
            dataset_path=dataset_path,
            estimator_type=estimator_type,
            train_fraction=train_fraction,
            random_state=random_state,
            min_horizon_hours=min_horizon_hours,
            max_horizon_hours=max_horizon_hours,
        )
        trade_covered_summary = summary["regime_analysis"].get(REGIME_TRADE_COVERED, {})
        if trade_covered_summary.get("available"):
            summary["whale_lift_gate_regime"] = REGIME_TRADE_COVERED
            summary["whale_lift_demonstrated"] = bool(trade_covered_summary.get("whale_lift_demonstrated"))
            cold_start_summary = summary["regime_analysis"].get(REGIME_COLD_START, {})
            if summary["whale_lift_demonstrated"]:
                summary["interpretation"] = "Whale lift is judged on the trade-covered regime and is demonstrated there."
            else:
                trade_price_rule_accuracy = trade_covered_summary.get("price_rule_accuracy")
                cold_start_price_rule_accuracy = cold_start_summary.get("price_rule_accuracy")
                summary["interpretation"] = (
                    "Whale lift is judged on the trade-covered regime. "
                    f"Trade-covered rows remain nearly price-determined (price_rule_accuracy={trade_price_rule_accuracy}), "
                    f"while cold-start rows remain neutral (price_rule_accuracy={cold_start_price_rule_accuracy}), "
                    "so whale lift beyond price is not demonstrated."
                )
        else:
            summary["whale_lift_gate_regime"] = REGIME_ALL
    else:
        summary["whale_lift_gate_regime"] = regime

    with analysis_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "analysis_path": str(analysis_path),
        "summary": summary,
    }


def train_market_outcome_baseline(
    *,
    dataset_path: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    feature_importance_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
) -> dict[str, Any]:
    """Train the grouped time-aware Random Forest market outcome baseline."""
    return train_market_model(
        dataset_path=dataset_path or DEFAULT_DATASET_PATH,
        model_path=model_path or DEFAULT_MODEL_PATH,
        metrics_path=metrics_path or DEFAULT_METRICS_PATH,
        feature_importance_path=feature_importance_path or DEFAULT_IMPORTANCE_PATH,
        task=TASK_MARKET_OUTCOME,
        estimator_type="random_forest",
        feature_set="full",
        evaluation_mode="single_split",
        train_fraction=train_fraction,
        random_state=random_state,
    )


def train_market_outcome_lightgbm(
    *,
    dataset_path: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    feature_importance_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
) -> dict[str, Any]:
    """Train the grouped time-aware LightGBM market outcome model."""
    return train_market_model(
        dataset_path=dataset_path or DEFAULT_DATASET_PATH,
        model_path=model_path or DEFAULT_LIGHTGBM_MODEL_PATH,
        metrics_path=metrics_path or DEFAULT_LIGHTGBM_METRICS_PATH,
        feature_importance_path=feature_importance_path or DEFAULT_LIGHTGBM_IMPORTANCE_PATH,
        task=TASK_MARKET_OUTCOME,
        estimator_type="lightgbm",
        feature_set="full",
        evaluation_mode="single_split",
        train_fraction=train_fraction,
        random_state=random_state,
    )
