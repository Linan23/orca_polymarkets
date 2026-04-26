"""Validate the LightGBM market outcome path against the current market dataset."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.ml.market_baseline_model import (
    compare_market_model_families,
    train_market_outcome_lightgbm,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the LightGBM market outcome path.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the dataset file is missing or empty.")
    return parser.parse_args()


def _csv_row_count(path: Path) -> int:
    """Return the row count for a CSV file, excluding the header."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = DEFAULT_DATASET_PATH
    model_path = Path("data_platform/runtime/ml/test_market_outcome_lightgbm_model.pkl")
    metrics_path = Path("data_platform/runtime/ml/test_market_outcome_lightgbm_metrics.json")
    importance_path = Path("data_platform/runtime/ml/test_market_outcome_lightgbm_feature_importance.csv")
    comparison_path = Path("data_platform/runtime/ml/test_market_model_family_comparison.json")

    row_count = _csv_row_count(dataset_path) if dataset_path.exists() else 0
    checks: list[dict[str, Any]] = [
        {
            "name": "dataset_available",
            "ok": (not args.require_data) or (dataset_path.exists() and row_count > 0),
            "dataset_path": str(dataset_path),
            "row_count": row_count,
        }
    ]

    training_summary = train_market_outcome_lightgbm(
        dataset_path=dataset_path,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=importance_path,
    )
    metrics = training_summary["metrics"]
    checks.append(
        {
            "name": "lightgbm_metrics_present",
            "ok": metrics["accuracy"] >= 0 and metrics["roc_auc"] >= 0,
            "accuracy": metrics["accuracy"],
            "roc_auc": metrics["roc_auc"],
        }
    )
    checks.append(
        {
            "name": "lightgbm_beats_majority_baseline",
            "ok": metrics["accuracy"] >= metrics["baseline_accuracy"],
            "accuracy": metrics["accuracy"],
            "baseline_accuracy": metrics["baseline_accuracy"],
        }
    )

    comparison_summary = compare_market_model_families(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
    )["summary"]
    checks.append(
        {
            "name": "model_family_comparison_present",
            "ok": comparison_summary["random_forest"]["accuracy"] >= 0 and comparison_summary["lightgbm"]["accuracy"] >= 0,
            "random_forest_accuracy": comparison_summary["random_forest"]["accuracy"],
            "lightgbm_accuracy": comparison_summary["lightgbm"]["accuracy"],
        }
    )
    transition_gate = comparison_summary.get("transition_gate") or {}
    checks.append(
        {
            "name": "lightgbm_declared_primary",
            "ok": comparison_summary.get("primary_model") == "lightgbm",
            "primary_model": comparison_summary.get("primary_model"),
        }
    )
    checks.append(
        {
            "name": "rolling_gate_metrics_present",
            "ok": transition_gate.get("lightgbm_rolling", {}).get("roc_auc") is not None
            and transition_gate.get("lightgbm_rolling", {}).get("log_loss") is not None
            and transition_gate.get("random_forest_rolling", {}).get("roc_auc") is not None
            and transition_gate.get("random_forest_rolling", {}).get("log_loss") is not None,
            "lightgbm_rolling": transition_gate.get("lightgbm_rolling"),
            "random_forest_rolling": transition_gate.get("random_forest_rolling"),
        }
    )

    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks, "lift": comparison_summary["lift"]}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
