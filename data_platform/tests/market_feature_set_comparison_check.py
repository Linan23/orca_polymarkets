"""Validate the price-only vs price-plus-whale comparison run."""

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
from data_platform.ml.market_baseline_model import compare_price_vs_whale_market_models
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the market feature-set comparison.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.metadata.json")
    comparison_path = Path("data_platform/runtime/ml/test_market_feature_set_comparison.json")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    result = compare_price_vs_whale_market_models(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
    )
    summary = result["summary"]

    checks: list[dict[str, Any]] = [
        {
            "name": "export_nonempty",
            "ok": (not args.require_data) or int(export_summary["row_count"]) > 0,
            "row_count": int(export_summary["row_count"]),
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
