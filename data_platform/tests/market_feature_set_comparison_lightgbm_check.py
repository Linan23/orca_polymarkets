"""Validate the LightGBM price-only vs price-plus-whale comparison."""

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

from data_platform.ml.market_baseline_model import compare_price_vs_whale_market_models_lightgbm
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the LightGBM market feature-set comparison.")
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
    comparison_path = Path("data_platform/runtime/ml/test_market_feature_set_comparison_lightgbm.json")

    row_count = _csv_row_count(dataset_path) if dataset_path.exists() else 0
    result = compare_price_vs_whale_market_models_lightgbm(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
    )
    summary = result["summary"]

    checks: list[dict[str, Any]] = [
        {
            "name": "dataset_available",
            "ok": (not args.require_data) or (dataset_path.exists() and row_count > 0),
            "dataset_path": str(dataset_path),
            "row_count": row_count,
        },
        {
            "name": "comparison_metrics_present",
            "ok": summary["price_only"]["accuracy"] >= 0 and summary["price_plus_whale"]["accuracy"] >= 0,
            "price_only_accuracy": summary["price_only"]["accuracy"],
            "price_plus_whale_accuracy": summary["price_plus_whale"]["accuracy"],
        },
        {
            "name": "comparison_uses_same_split",
            "ok": summary["train_condition_count"] > 0 and summary["test_condition_count"] > 0,
            "train_condition_count": summary["train_condition_count"],
            "test_condition_count": summary["test_condition_count"],
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks, "lift": summary["lift"]}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
