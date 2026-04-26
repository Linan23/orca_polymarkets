"""Validate whale feature sparsity and ablation reporting."""

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
from data_platform.ml.market_baseline_model import analyze_whale_feature_ablation
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the whale feature ablation report.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def _window_checks(window_name: str, window_summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validation checks for one movement ablation window."""
    feature_sets = window_summary.get("feature_sets", {})
    lifts = window_summary.get("lift_vs_price_only", {})
    return [
        {
            "name": f"{window_name}_feature_sets_present",
            "ok": all(
                name in feature_sets
                for name in (
                    "price_only",
                    "price_plus_all_whale",
                    "price_plus_selected_whale",
                    "price_plus_without_recent_whale",
                    "price_plus_recent_whale_only",
                )
            ),
            "feature_set_names": list(feature_sets.keys()),
        },
        {
            "name": f"{window_name}_rolling_metrics_present",
            "ok": feature_sets.get("price_only", {}).get("rolling_average", {}).get("rmse") is not None
            and feature_sets.get("price_plus_all_whale", {}).get("rolling_average", {}).get("rmse") is not None,
            "price_only_rolling_rmse": feature_sets.get("price_only", {}).get("rolling_average", {}).get("rmse"),
            "price_plus_all_rolling_rmse": feature_sets.get("price_plus_all_whale", {})
            .get("rolling_average", {})
            .get("rmse"),
        },
        {
            "name": f"{window_name}_lift_gate_present",
            "ok": "price_plus_all_whale" in lifts
            and "minimum_required_rolling_rmse_delta" in lifts.get("price_plus_all_whale", {}),
            "lift_gate": lifts.get("price_plus_all_whale"),
        },
        {
            "name": f"{window_name}_selected_whale_feature_selection_present",
            "ok": feature_sets.get("price_plus_selected_whale", {})
            .get("feature_selection", {})
            .get("mode")
            == "training_correlation"
            and feature_sets.get("price_plus_selected_whale", {})
            .get("feature_selection", {})
            .get("rolling", {})
            .get("fold_count", 0)
            > 0,
            "feature_selection": feature_sets.get("price_plus_selected_whale", {}).get("feature_selection"),
        },
    ]


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_ablation_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_ablation_market_snapshot_features.metadata.json")
    report_path = Path("data_platform/runtime/ml/test_market_whale_feature_ablation_report.json")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    result = analyze_whale_feature_ablation(
        dataset_path=dataset_path,
        report_path=report_path,
        estimator_type="random_forest",
    )
    summary = result["summary"]
    windows = summary.get("windows", {})
    feature_groups = summary.get("feature_group_summaries", {})
    checks: list[dict[str, Any]] = [
        {
            "name": "export_nonempty",
            "ok": (not args.require_data) or int(export_summary["row_count"]) > 0,
            "row_count": int(export_summary["row_count"]),
        },
        {
            "name": "report_path_written",
            "ok": report_path.exists(),
            "report_path": str(report_path),
        },
        {
            "name": "feature_group_summaries_present",
            "ok": all(
                name in feature_groups
                for name in (
                    "recent_pressure",
                    "weighted_pressure",
                    "scored_whale_weighted_pressure",
                    "trusted_whale_weighted_pressure",
                    "all_whale",
                )
            ),
            "feature_group_names": list(feature_groups.keys()),
        },
        {
            "name": "recent_pressure_group_has_columns",
            "ok": int(feature_groups.get("recent_pressure", {}).get("feature_count", 0)) > 0,
            "recent_pressure_summary": feature_groups.get("recent_pressure"),
        },
        {
            "name": "sparsity_lists_present",
            "ok": isinstance(summary.get("sparse_features"), list) and isinstance(summary.get("dense_features"), list),
            "sparse_count": len(summary.get("sparse_features", [])),
            "dense_count": len(summary.get("dense_features", [])),
        },
        {
            "name": "windows_present",
            "ok": all(name in windows for name in ("12h", "24h")),
            "window_names": list(windows.keys()),
        },
        {
            "name": "overall_gate_present",
            "ok": isinstance(summary.get("overall_material_whale_lift_demonstrated"), bool),
            "overall_material_whale_lift_demonstrated": summary.get("overall_material_whale_lift_demonstrated"),
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
                "report_path": result["report_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
