"""Validate residual whale movement model-family comparison."""

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

from data_platform.ml.market_baseline_model import compare_market_movement_residual_model_families
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate residual model-family comparison.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the current market dataset is missing or empty.")
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
    comparison_path = Path("data_platform/runtime/ml/test_market_movement_residual_model_comparison.json")
    markdown_path = Path("data_platform/runtime/ml/test_week10_11_market_movement_residual_model_comparison.md")

    row_count = _csv_row_count(dataset_path) if dataset_path.exists() else 0
    result = compare_market_movement_residual_model_families(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
        markdown_path=markdown_path,
        estimator_types=("random_forest", "ridge"),
        selector_thresholds=(0.02,),
        selector_max_features=(8,),
    )
    summary = result["summary"]
    models = summary.get("models", {})
    recommendation = summary.get("recommendation", {})

    checks: list[dict[str, Any]] = [
        {
            "name": "dataset_available",
            "ok": (not args.require_data) or (dataset_path.exists() and row_count > 0),
            "dataset_path": str(dataset_path),
            "row_count": row_count,
        },
        {
            "name": "comparison_path_written",
            "ok": comparison_path.exists(),
            "comparison_path": str(comparison_path),
        },
        {
            "name": "markdown_path_written",
            "ok": markdown_path.exists()
            and "Residual Model Family Comparison" in markdown_path.read_text(encoding="utf-8"),
            "markdown_path": str(markdown_path),
        },
        {
            "name": "estimators_present",
            "ok": set(models) == {"random_forest", "ridge"},
            "estimators": list(models),
        },
        {
            "name": "recommendation_present",
            "ok": recommendation.get("default_estimator") in models
            and isinstance(recommendation.get("window_recommendations"), dict),
            "recommendation": recommendation,
        },
        {
            "name": "comparison_criteria_present",
            "ok": bool(summary.get("comparison_criteria")),
            "comparison_criteria": summary.get("comparison_criteria"),
        },
    ]
    for estimator_type, model in models.items():
        windows = model.get("windows", {})
        checks.append(
            {
                "name": f"{estimator_type}_windows_present",
                "ok": all(window in windows for window in ("12h", "24h")),
                "windows": list(windows),
            }
        )
        checks.append(
            {
                "name": f"{estimator_type}_score_present",
                "ok": model.get("score", {}).get("required_window_count") == 2
                and "mean_selected_rmse_delta" in model.get("score", {}),
                "score": model.get("score"),
            }
        )

    ok = all(check["ok"] for check in checks)
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "comparison_path": result["comparison_path"],
                "markdown_path": result["markdown_path"],
                "detail_report_dir": result["detail_report_dir"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
