"""Grouped time-aware baseline models for the market-level ML dataset."""

from __future__ import annotations

import csv
import json
import pickle
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_platform.ml.market_dataset_builder import (
    DATASET_VERSION,
    DEFAULT_DATASET_PATH,
    FEATURE_COLUMNS,
    GROUP_KEY_COLUMN,
    TARGET_COLUMN,
    WHALE_FEATURE_COLUMNS,
)


MODEL_VERSION = "market_outcome_random_forest_v2"
DEFAULT_MODEL_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_model.pkl")
DEFAULT_METRICS_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_metrics.json")
DEFAULT_IMPORTANCE_PATH = Path("data_platform/runtime/ml/market_outcome_baseline_feature_importance.csv")
DEFAULT_COMPARISON_PATH = Path("data_platform/runtime/ml/market_feature_set_comparison.json")
DEFAULT_LIGHTGBM_FEATURE_SET_COMPARISON_PATH = Path("data_platform/runtime/ml/market_feature_set_comparison_lightgbm.json")
LIGHTGBM_MODEL_VERSION = "market_outcome_lightgbm_v1"
DEFAULT_LIGHTGBM_MODEL_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_model.pkl")
DEFAULT_LIGHTGBM_METRICS_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_metrics.json")
DEFAULT_LIGHTGBM_IMPORTANCE_PATH = Path("data_platform/runtime/ml/market_outcome_lightgbm_feature_importance.csv")
DEFAULT_MODEL_FAMILY_COMPARISON_PATH = Path("data_platform/runtime/ml/market_model_family_comparison.json")

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
PRICE_BASELINE_FEATURE_COLUMNS = TIME_CONTEXT_FEATURE_COLUMNS + PRICE_FEATURE_COLUMNS
PRICE_PLUS_WHALE_FEATURE_COLUMNS = PRICE_BASELINE_FEATURE_COLUMNS + WHALE_FEATURE_COLUMNS


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
            parsed["market_end_time"] = datetime.fromisoformat(str(row["market_end_time"]))
            rows.append(parsed)
    return rows


def _evaluate_feature_set(
    *,
    train_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    model_version: str,
    random_state: int,
    estimator_type: str = "random_forest",
) -> tuple[Any, dict[str, Any], list[tuple[str, float]]]:
    """Fit one model on a fixed grouped split and return metrics plus feature importances."""
    from sklearn.dummy import DummyClassifier
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

    y_train = [int(row[TARGET_COLUMN]) for row in train_rows]
    y_test = [int(row[TARGET_COLUMN]) for row in test_rows]
    if len(set(y_train)) < 2 or len(set(y_test)) < 2:
        raise RuntimeError("Grouped time split must leave both target classes present in train and test.")

    x_train = [[float(row[column]) for column in feature_columns] for row in train_rows]
    x_test = [[float(row[column]) for column in feature_columns] for row in test_rows]

    baseline_model = DummyClassifier(strategy="most_frequent")
    baseline_model.fit(x_train, y_train)
    baseline_predictions = baseline_model.predict(x_test)
    baseline_accuracy = accuracy_score(y_test, baseline_predictions)

    if estimator_type == "random_forest":
        from sklearn.ensemble import RandomForestClassifier

        model = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=3,
            random_state=random_state,
            class_weight="balanced_subsample",
            n_jobs=-1,
        )
    elif estimator_type == "lightgbm":
        try:
            from lightgbm import LGBMClassifier
        except ImportError as exc:  # pragma: no cover - runtime dependency guard
            raise RuntimeError(
                "lightgbm is required for this model path. Run `pip install -r requirements.txt` first."
            ) from exc

        model = LGBMClassifier(
            objective="binary",
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=6,
            min_child_samples=5,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=1.0,
            class_weight="balanced",
            random_state=random_state,
            n_jobs=-1,
            importance_type="gain",
            verbosity=-1,
        )
    else:  # pragma: no cover - invalid caller path
        raise RuntimeError(f"Unsupported estimator_type: {estimator_type}")

    model.fit(x_train, y_train)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMClassifier was fitted with feature names",
            category=UserWarning,
        )
        predictions = model.predict(x_test)
        probabilities = model.predict_proba(x_test)[:, 1]

    metrics = {
        "dataset_version": DATASET_VERSION,
        "model_version": model_version,
        "target_column": TARGET_COLUMN,
        "feature_columns": list(feature_columns),
        "estimator_type": estimator_type,
        "baseline_accuracy": round(float(baseline_accuracy), 6),
        "accuracy": round(float(accuracy_score(y_test, predictions)), 6),
        "precision": round(float(precision_score(y_test, predictions, zero_division=0)), 6),
        "recall": round(float(recall_score(y_test, predictions, zero_division=0)), 6),
        "f1": round(float(f1_score(y_test, predictions, zero_division=0)), 6),
        "roc_auc": round(float(roc_auc_score(y_test, probabilities)), 6),
    }

    feature_importances = list(zip(feature_columns, model.feature_importances_, strict=True))
    feature_importances.sort(key=lambda item: item[1], reverse=True)
    metrics["top_features"] = [
        {"feature": feature, "importance": round(float(importance), 6)}
        for feature, importance in feature_importances[:10]
    ]
    return model, metrics, feature_importances


def _run_model_training(
    *,
    dataset_path: Path,
    model_path: Path,
    metrics_path: Path,
    feature_importance_path: Path,
    feature_columns: tuple[str, ...],
    model_version: str,
    estimator_type: str,
    train_fraction: float,
    random_state: int,
) -> dict[str, Any]:
    """Train one grouped time-aware model and persist its artifacts."""
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    feature_importance_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(dataset_path)
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path}. Export the market ML dataset first.")

    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    model, metrics, feature_importances = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=feature_columns,
        model_version=model_version,
        random_state=random_state,
        estimator_type=estimator_type,
    )

    metrics.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dataset_path": str(dataset_path),
            "row_count": len(rows),
            "train_rows": len(train_rows),
            "test_rows": len(test_rows),
            "train_condition_count": len(train_conditions),
            "test_condition_count": len(test_conditions),
            "train_fraction": train_fraction,
            "random_state": random_state,
            "train_end_time_range": {
                "min": min(row["market_end_time"] for row in train_rows).isoformat(),
                "max": max(row["market_end_time"] for row in train_rows).isoformat(),
            },
            "test_end_time_range": {
                "min": min(row["market_end_time"] for row in test_rows).isoformat(),
                "max": max(row["market_end_time"] for row in test_rows).isoformat(),
            },
        }
    )

    with model_path.open("wb") as handle:
        pickle.dump(model, handle)

    with metrics_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")

    with feature_importance_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["feature", "importance"])
        for feature, importance in feature_importances:
            writer.writerow([feature, round(float(importance), 10)])

    return {
        "model_path": str(model_path),
        "metrics_path": str(metrics_path),
        "feature_importance_path": str(feature_importance_path),
        "metrics": metrics,
    }


def _grouped_time_split(rows: list[dict[str, Any]], train_fraction: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[str]]:
    """Split rows by condition, keeping older conditions in train and newer ones in test."""
    if not 0 < train_fraction < 1:
        raise RuntimeError("--train-fraction must be between 0 and 1.")

    grouped_end_times: dict[str, datetime] = {}
    for row in rows:
        condition_ref = str(row[GROUP_KEY_COLUMN])
        market_end_time = row["market_end_time"]
        existing = grouped_end_times.get(condition_ref)
        if existing is None or market_end_time < existing:
            grouped_end_times[condition_ref] = market_end_time

    ordered_groups = sorted(grouped_end_times.items(), key=lambda item: item[1])
    if len(ordered_groups) < 2:
        raise RuntimeError("At least two resolved conditions are required for a grouped time split.")

    train_group_count = int(len(ordered_groups) * train_fraction)
    train_group_count = max(1, min(train_group_count, len(ordered_groups) - 1))
    train_conditions = [condition_ref for condition_ref, _ in ordered_groups[:train_group_count]]
    test_conditions = [condition_ref for condition_ref, _ in ordered_groups[train_group_count:]]
    train_condition_set = set(train_conditions)
    test_condition_set = set(test_conditions)
    train_rows = [row for row in rows if str(row[GROUP_KEY_COLUMN]) in train_condition_set]
    test_rows = [row for row in rows if str(row[GROUP_KEY_COLUMN]) in test_condition_set]
    return train_rows, test_rows, train_conditions, test_conditions


def train_market_outcome_baseline(
    *,
    dataset_path: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    feature_importance_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
) -> dict[str, Any]:
    """Train the grouped time-aware market outcome baseline."""
    try:
        import sklearn  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for ML baseline training. Run `pip install -r requirements.txt` first."
        ) from exc

    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    model_path = model_path or DEFAULT_MODEL_PATH
    metrics_path = metrics_path or DEFAULT_METRICS_PATH
    feature_importance_path = feature_importance_path or DEFAULT_IMPORTANCE_PATH
    return _run_model_training(
        dataset_path=dataset_path,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=feature_importance_path,
        feature_columns=FEATURE_COLUMNS,
        model_version=MODEL_VERSION,
        estimator_type="random_forest",
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
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    model_path = model_path or DEFAULT_LIGHTGBM_MODEL_PATH
    metrics_path = metrics_path or DEFAULT_LIGHTGBM_METRICS_PATH
    feature_importance_path = feature_importance_path or DEFAULT_LIGHTGBM_IMPORTANCE_PATH
    return _run_model_training(
        dataset_path=dataset_path,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=feature_importance_path,
        feature_columns=FEATURE_COLUMNS,
        model_version=LIGHTGBM_MODEL_VERSION,
        estimator_type="lightgbm",
        train_fraction=train_fraction,
        random_state=random_state,
    )


def compare_price_vs_whale_market_models(
    *,
    dataset_path: Path | None = None,
    comparison_path: Path | None = None,
    train_fraction: float = 0.75,
    random_state: int = 42,
) -> dict[str, Any]:
    """Compare a price/context model against a price-plus-whale model on the same grouped split."""
    try:
        import sklearn  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for ML baseline training. Run `pip install -r requirements.txt` first."
        ) from exc

    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(dataset_path)
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path}. Export the market ML dataset first.")

    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    _, price_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=PRICE_BASELINE_FEATURE_COLUMNS,
        model_version="market_price_only_random_forest_v1",
        random_state=random_state,
    )
    _, whale_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        model_version="market_price_plus_whale_random_forest_v1",
        random_state=random_state,
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "row_count": len(rows),
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "train_condition_count": len(train_conditions),
        "test_condition_count": len(test_conditions),
        "train_fraction": train_fraction,
        "random_state": random_state,
        "assumptions": [
            "Both models use the same grouped time split by condition_ref.",
            "The price-plus-whale model adds whale participation features to the same price/context baseline.",
            "Whale participation features are computed from trade and resolved-market history available on or before each observation cutoff.",
            "Historical whale exposure uses an open-share proxy valued at average buy price.",
        ],
        "price_only": price_metrics,
        "price_plus_whale": whale_metrics,
        "lift": {
            "accuracy_delta": round(whale_metrics["accuracy"] - price_metrics["accuracy"], 6),
            "roc_auc_delta": round(whale_metrics["roc_auc"] - price_metrics["roc_auc"], 6),
            "f1_delta": round(whale_metrics["f1"] - price_metrics["f1"], 6),
        },
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
) -> dict[str, Any]:
    """Compare LightGBM price/context and price-plus-whale models on the same grouped split."""
    try:
        import sklearn  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for ML baseline training. Run `pip install -r requirements.txt` first."
        ) from exc

    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_LIGHTGBM_FEATURE_SET_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(dataset_path)
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path}. Export the market ML dataset first.")

    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    _, price_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=PRICE_BASELINE_FEATURE_COLUMNS,
        model_version="market_price_only_lightgbm_v1",
        random_state=random_state,
        estimator_type="lightgbm",
    )
    _, whale_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=PRICE_PLUS_WHALE_FEATURE_COLUMNS,
        model_version="market_price_plus_whale_lightgbm_v1",
        random_state=random_state,
        estimator_type="lightgbm",
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "row_count": len(rows),
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "train_condition_count": len(train_conditions),
        "test_condition_count": len(test_conditions),
        "train_fraction": train_fraction,
        "random_state": random_state,
        "assumptions": [
            "Both LightGBM models use the same grouped time split by condition_ref.",
            "The price-plus-whale model adds whale participation features to the same price/context baseline.",
            "Whale participation features are computed from trade and resolved-market history available on or before each observation cutoff.",
            "Historical whale exposure uses an open-share proxy valued at average buy price.",
        ],
        "price_only": price_metrics,
        "price_plus_whale": whale_metrics,
        "lift": {
            "accuracy_delta": round(whale_metrics["accuracy"] - price_metrics["accuracy"], 6),
            "roc_auc_delta": round(whale_metrics["roc_auc"] - price_metrics["roc_auc"], 6),
            "f1_delta": round(whale_metrics["f1"] - price_metrics["f1"], 6),
        },
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
) -> dict[str, Any]:
    """Compare Random Forest and LightGBM on the same grouped time split and feature set."""
    try:
        import sklearn  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for ML baseline training. Run `pip install -r requirements.txt` first."
        ) from exc

    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    comparison_path = comparison_path or DEFAULT_MODEL_FAMILY_COMPARISON_PATH
    comparison_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(dataset_path)
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path}. Export the market ML dataset first.")

    train_rows, test_rows, train_conditions, test_conditions = _grouped_time_split(rows, train_fraction)
    _, random_forest_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=FEATURE_COLUMNS,
        model_version=MODEL_VERSION,
        random_state=random_state,
        estimator_type="random_forest",
    )
    _, lightgbm_metrics, _ = _evaluate_feature_set(
        train_rows=train_rows,
        test_rows=test_rows,
        feature_columns=FEATURE_COLUMNS,
        model_version=LIGHTGBM_MODEL_VERSION,
        random_state=random_state,
        estimator_type="lightgbm",
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "dataset_version": DATASET_VERSION,
        "row_count": len(rows),
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "train_condition_count": len(train_conditions),
        "test_condition_count": len(test_conditions),
        "train_fraction": train_fraction,
        "random_state": random_state,
        "assumptions": [
            "Both models use the same grouped time split by condition_ref.",
            "Both models use the same full market feature set from the current dataset version.",
            "This comparison is for model-family selection only; feature engineering remains the larger bottleneck.",
        ],
        "random_forest": random_forest_metrics,
        "lightgbm": lightgbm_metrics,
        "lift": {
            "accuracy_delta": round(lightgbm_metrics["accuracy"] - random_forest_metrics["accuracy"], 6),
            "roc_auc_delta": round(lightgbm_metrics["roc_auc"] - random_forest_metrics["roc_auc"], 6),
            "f1_delta": round(lightgbm_metrics["f1"] - random_forest_metrics["f1"], 6),
        },
    }

    with comparison_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "comparison_path": str(comparison_path),
        "summary": summary,
    }
