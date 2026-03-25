"""Validate the grouped time-aware market outcome baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.db.session import session_scope
from data_platform.ml.market_baseline_model import train_market_outcome_baseline
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the grouped time-aware market outcome baseline.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.metadata.json")
    model_path = Path("data_platform/runtime/ml/test_market_outcome_baseline_model.pkl")
    metrics_path = Path("data_platform/runtime/ml/test_market_outcome_baseline_metrics.json")
    importance_path = Path("data_platform/runtime/ml/test_market_outcome_baseline_feature_importance.csv")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    checks: list[dict[str, Any]] = []
    row_count = int(export_summary["row_count"])
    checks.append({"name": "export_nonempty", "ok": (not args.require_data) or row_count > 0, "row_count": row_count})

    training_summary = train_market_outcome_baseline(
        dataset_path=dataset_path,
        model_path=model_path,
        metrics_path=metrics_path,
        feature_importance_path=importance_path,
    )
    metrics = training_summary["metrics"]
    checks.append(
        {
            "name": "baseline_metrics_present",
            "ok": metrics["accuracy"] >= 0 and metrics["roc_auc"] >= 0,
            "accuracy": metrics["accuracy"],
            "roc_auc": metrics["roc_auc"],
        }
    )
    checks.append(
        {
            "name": "grouped_split_present",
            "ok": metrics["train_condition_count"] > 0 and metrics["test_condition_count"] > 0,
            "train_condition_count": metrics["train_condition_count"],
            "test_condition_count": metrics["test_condition_count"],
        }
    )
    checks.append(
        {
            "name": "model_beats_majority_baseline",
            "ok": metrics["accuracy"] >= metrics["baseline_accuracy"],
            "accuracy": metrics["accuracy"],
            "baseline_accuracy": metrics["baseline_accuracy"],
        }
    )

    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
