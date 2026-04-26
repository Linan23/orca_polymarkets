"""Validate residual whale movement analysis."""

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
from data_platform.ml.market_baseline_model import analyze_market_movement_residuals
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate residual whale movement analysis.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def _window_checks(window_name: str, window_summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validation checks for one residual movement window."""
    configs = window_summary.get("configs", {})
    first_config = next(iter(configs.values()), {})
    rolling = first_config.get("rolling", {})
    return [
        {
            "name": f"{window_name}_configs_present",
            "ok": bool(configs),
            "config_names": list(configs.keys()),
        },
        {
            "name": f"{window_name}_rolling_residual_metrics_present",
            "ok": rolling.get("price_only_average", {}).get("rmse") is not None
            and rolling.get("residual_corrected_average", {}).get("rmse") is not None
            and "rmse_delta" in rolling.get("lift_vs_price_only", {}),
            "price_only_average": rolling.get("price_only_average"),
            "residual_corrected_average": rolling.get("residual_corrected_average"),
        },
        {
            "name": f"{window_name}_stability_present",
            "ok": rolling.get("feature_selection_stability", {}).get("fold_count", 0) > 0
            and "stable_whale_feature_count" in rolling.get("feature_selection_stability", {}),
            "feature_selection_stability": rolling.get("feature_selection_stability"),
        },
        {
            "name": f"{window_name}_segment_diagnostics_present",
            "ok": bool(rolling.get("segment_diagnostics", {}).get("research_focus")),
            "segments": rolling.get("segment_diagnostics", {}).get("research_focus"),
        },
        {
            "name": f"{window_name}_fold_delta_summary_present",
            "ok": rolling.get("fold_rmse_delta_summary", {}).get("available") is True
            and "normal_approx_95ci_low" in rolling.get("fold_rmse_delta_summary", {}),
            "fold_rmse_delta_summary": rolling.get("fold_rmse_delta_summary"),
        },
        {
            "name": f"{window_name}_recommendation_present",
            "ok": window_summary.get("recommendation", {}).get("available") is True
            and "whale_lift_demonstrated" in window_summary.get("recommendation", {})
            and "selected_config_requires_recurring_whale_features" in window_summary.get("recommendation", {}),
            "recommendation": window_summary.get("recommendation"),
        },
    ]


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_residual_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_residual_market_snapshot_features.metadata.json")
    report_path = Path("data_platform/runtime/ml/test_market_movement_residual_report.json")
    markdown_path = Path("data_platform/runtime/ml/test_week10_11_market_movement_residual_report.md")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    result = analyze_market_movement_residuals(
        dataset_path=dataset_path,
        report_path=report_path,
        markdown_path=markdown_path,
        estimator_type="random_forest",
        selector_thresholds=(0.02,),
        selector_max_features=(8,),
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
            "name": "report_path_written",
            "ok": report_path.exists(),
            "report_path": str(report_path),
        },
        {
            "name": "markdown_path_written",
            "ok": markdown_path.exists() and "Residual Whale Movement" in markdown_path.read_text(encoding="utf-8"),
            "markdown_path": str(markdown_path),
        },
        {
            "name": "windows_present",
            "ok": all(name in windows for name in ("12h", "24h")),
            "window_names": list(windows.keys()),
        },
        {
            "name": "overall_gate_present",
            "ok": isinstance(summary.get("overall_residual_whale_lift_demonstrated"), bool),
            "overall_residual_whale_lift_demonstrated": summary.get("overall_residual_whale_lift_demonstrated"),
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
                "markdown_path": result["markdown_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
