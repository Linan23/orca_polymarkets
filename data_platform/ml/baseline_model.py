"""Baseline classifier training on the resolved user/market ML dataset."""

from __future__ import annotations

import csv
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_platform.ml.dataset_builder import (
    DATASET_VERSION,
    DEFAULT_DATASET_PATH,
    MODEL_FEATURE_COLUMNS,
    TARGET_COLUMN,
)


MODEL_VERSION = "profitability_random_forest_v1"
DEFAULT_MODEL_PATH = Path("data_platform/runtime/ml/profitability_baseline_model.pkl")
DEFAULT_METRICS_PATH = Path("data_platform/runtime/ml/profitability_baseline_metrics.json")
DEFAULT_IMPORTANCE_PATH = Path("data_platform/runtime/ml/profitability_baseline_feature_importance.csv")


def _load_training_rows(dataset_path: Path) -> list[dict[str, Any]]:
    """Load CSV rows and coerce numeric feature values for model training."""
    rows: list[dict[str, Any]] = []
    with dataset_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            parsed: dict[str, Any] = dict(row)
            for column in MODEL_FEATURE_COLUMNS:
                parsed[column] = float(row.get(column, 0) or 0)
            parsed[TARGET_COLUMN] = int(float(row.get(TARGET_COLUMN, 0) or 0))
            rows.append(parsed)
    return rows


def train_profitability_baseline(
    *,
    dataset_path: Path | None = None,
    model_path: Path | None = None,
    metrics_path: Path | None = None,
    feature_importance_path: Path | None = None,
    test_size: float = 0.25,
    random_state: int = 42,
) -> dict[str, Any]:
    """Train a first-pass profitability classifier and persist the outputs."""
    try:
        from sklearn.dummy import DummyClassifier
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
        from sklearn.model_selection import train_test_split
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "scikit-learn is required for ML baseline training. Run `pip install -r requirements.txt` first."
        ) from exc

    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    model_path = model_path or DEFAULT_MODEL_PATH
    metrics_path = metrics_path or DEFAULT_METRICS_PATH
    feature_importance_path = feature_importance_path or DEFAULT_IMPORTANCE_PATH
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    feature_importance_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_training_rows(dataset_path)
    if not rows:
        raise RuntimeError(f"No rows were found in {dataset_path}. Export the ML dataset first.")

    y = [int(row[TARGET_COLUMN]) for row in rows]
    positive_count = sum(y)
    negative_count = len(y) - positive_count
    if positive_count == 0 or negative_count == 0:
        raise RuntimeError(
            "The dataset contains only one target class. The baseline trainer needs both positive and negative rows."
        )

    x = [[float(row[column]) for column in MODEL_FEATURE_COLUMNS] for row in rows]
    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )

    baseline_model = DummyClassifier(strategy="most_frequent")
    baseline_model.fit(x_train, y_train)
    baseline_predictions = baseline_model.predict(x_test)
    baseline_accuracy = accuracy_score(y_test, baseline_predictions)

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=5,
        random_state=random_state,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )
    model.fit(x_train, y_train)

    predictions = model.predict(x_test)
    probabilities = model.predict_proba(x_test)[:, 1]
    metrics = {
        "dataset_version": DATASET_VERSION,
        "model_version": MODEL_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "row_count": len(rows),
        "positive_rows": positive_count,
        "negative_rows": negative_count,
        "positive_rate": round(positive_count / len(rows), 6),
        "train_rows": len(x_train),
        "test_rows": len(x_test),
        "test_size": test_size,
        "random_state": random_state,
        "target_column": TARGET_COLUMN,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "baseline_accuracy": round(float(baseline_accuracy), 6),
        "accuracy": round(float(accuracy_score(y_test, predictions)), 6),
        "precision": round(float(precision_score(y_test, predictions, zero_division=0)), 6),
        "recall": round(float(recall_score(y_test, predictions, zero_division=0)), 6),
        "f1": round(float(f1_score(y_test, predictions, zero_division=0)), 6),
        "roc_auc": round(float(roc_auc_score(y_test, probabilities)), 6),
    }

    feature_importances = list(zip(MODEL_FEATURE_COLUMNS, model.feature_importances_, strict=True))
    feature_importances.sort(key=lambda item: item[1], reverse=True)
    metrics["top_features"] = [
        {"feature": feature, "importance": round(float(importance), 6)}
        for feature, importance in feature_importances[:10]
    ]

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
