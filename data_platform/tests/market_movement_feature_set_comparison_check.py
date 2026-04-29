"""Validate the 12h/24h movement feature-set comparison."""

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
from data_platform.ml.market_baseline_model import compare_price_vs_whale_market_movement_models
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the market movement feature-set comparison.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def _window_checks(window_name: str, window_summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validation checks for one movement window."""
    price_only = window_summary.get("price_only", {})
    price_plus_whale = window_summary.get("price_plus_whale", {})
    return [
        {
            "name": f"{window_name}_target_column_present",
            "ok": window_summary.get("target_column") == f"future_price_delta_{window_name}",
            "target_column": window_summary.get("target_column"),
        },
        {
            "name": f"{window_name}_feature_sets_present",
            "ok": all(name in window_summary for name in ("price_only", "whale_only", "price_plus_whale")),
        },
        {
            "name": f"{window_name}_metrics_present",
            "ok": price_only.get("rmse") is not None
            and price_plus_whale.get("rmse") is not None
            and price_only.get("mae") is not None
            and price_plus_whale.get("mae") is not None,
            "price_only_rmse": price_only.get("rmse"),
            "price_plus_whale_rmse": price_plus_whale.get("rmse"),
        },
        {
            "name": f"{window_name}_rolling_metrics_present",
            "ok": price_only.get("rolling_metrics", {}).get("fold_count", 0) > 0
            and price_plus_whale.get("rolling_metrics", {}).get("fold_count", 0) > 0,
            "price_only_folds": price_only.get("rolling_metrics", {}).get("fold_count", 0),
            "price_plus_whale_folds": price_plus_whale.get("rolling_metrics", {}).get("fold_count", 0),
        },
        {
            "name": f"{window_name}_lift_summary_present",
            "ok": "rmse_delta" in window_summary.get("lift_vs_price_only", {}),
            "lift_vs_price_only": window_summary.get("lift_vs_price_only"),
        },
    ]


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_movement_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_movement_market_snapshot_features.metadata.json")
    comparison_path = Path("data_platform/runtime/ml/test_market_movement_feature_set_comparison.json")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    result = compare_price_vs_whale_market_movement_models(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
        estimator_type="random_forest",
    )
    summary = result["summary"]
    windows = summary.get("windows", {})
    checks: list[dict[str, Any]] = [
        {
            "name": "export_nonempty",
            "ok": (not args.require_data) or int(export_summary["row_count"]) > 0,
            "row_count": int(export_summary["row_count"]),
        },
        {
            "name": "windows_present",
            "ok": all(name in windows for name in ("12h", "24h")),
            "window_names": list(windows.keys()),
        },
        {
            "name": "comparison_path_written",
            "ok": Path(result["comparison_path"]).exists(),
            "comparison_path": result["comparison_path"],
        },
    ]
    for window_name in ("12h", "24h"):
        checks.extend(_window_checks(window_name, windows.get(window_name, {})))

    ok = all(check["ok"] for check in checks)
    print(
        json.dumps(
            {
                "ok": ok,
                "checks": checks,
                "overall_whale_lift_demonstrated": summary.get("overall_whale_lift_demonstrated"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
