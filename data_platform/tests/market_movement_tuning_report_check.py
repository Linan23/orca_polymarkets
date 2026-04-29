"""Validate the Week 10-11 movement tuning report."""

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
from data_platform.ml.market_baseline_model import tune_market_movement_models
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the movement tuning report.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def _window_checks(window_name: str, window_summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Return validation checks for one movement tuning window."""
    profiles = window_summary.get("profiles", {})
    available_profiles = [
        profile
        for profile in profiles.values()
        if profile.get("available", True)
    ]
    first_profile = available_profiles[0] if available_profiles else {}
    price_only = first_profile.get("price_only", {})
    price_plus_whale = first_profile.get("price_plus_whale", {})
    lift_gate = first_profile.get("lift_vs_price_only", {})
    return [
        {
            "name": f"{window_name}_profile_present",
            "ok": bool(available_profiles),
            "profiles": list(profiles.keys()),
        },
        {
            "name": f"{window_name}_compact_metrics_present",
            "ok": price_only.get("rolling_average", {}).get("rmse") is not None
            and price_plus_whale.get("rolling_average", {}).get("rmse") is not None,
            "price_only_rolling_rmse": price_only.get("rolling_average", {}).get("rmse"),
            "price_plus_whale_rolling_rmse": price_plus_whale.get("rolling_average", {}).get("rmse"),
        },
        {
            "name": f"{window_name}_generalization_gate_present",
            "ok": "passes_generalization_gate" in lift_gate and "single_split_only_lift" in lift_gate,
            "lift_gate": lift_gate,
        },
        {
            "name": f"{window_name}_recommendation_present",
            "ok": window_summary.get("recommendation", {}).get("available") is True
            and "whale_lift_demonstrated" in window_summary.get("recommendation", {}),
            "recommendation": window_summary.get("recommendation"),
        },
    ]


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_tuning_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_tuning_market_snapshot_features.metadata.json")
    report_path = Path("data_platform/runtime/ml/test_market_movement_tuning_report.json")
    markdown_path = Path("data_platform/runtime/ml/test_week10_11_market_movement_report.md")

    with session_scope() as session:
        export_summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    result = tune_market_movement_models(
        dataset_path=dataset_path,
        report_path=report_path,
        markdown_path=markdown_path,
        profile_names=("rf_shallow",),
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
            "ok": markdown_path.exists() and "Week 10-11" in markdown_path.read_text(encoding="utf-8"),
            "markdown_path": str(markdown_path),
        },
        {
            "name": "windows_present",
            "ok": all(name in windows for name in ("12h", "24h")),
            "window_names": list(windows.keys()),
        },
        {
            "name": "overall_gate_present",
            "ok": isinstance(summary.get("overall_whale_lift_demonstrated"), bool),
            "overall_whale_lift_demonstrated": summary.get("overall_whale_lift_demonstrated"),
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
