"""Validate the residual whale-signal analysis pipeline."""

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

from data_platform.db.session import session_scope
from data_platform.ml.market_baseline_model import _load_training_rows, analyze_market_whale_signal
from data_platform.ml.market_dataset_builder import (
    DEFAULT_DATASET_PATH,
    PRICE_BASELINE_COLUMN,
    RESOLUTION_EDGE_COLUMN,
    export_market_snapshot_dataset,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the residual whale-signal analysis pipeline.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the dataset file is missing or empty.")
    return parser.parse_args()


def _dataset_scan(path: Path) -> tuple[int, list[str], list[float]]:
    """Return row count, CSV header, and resolution-edge values."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        values = [float(row.get(RESOLUTION_EDGE_COLUMN, 0) or 0) for row in reader]
    return len(values), fieldnames, values


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    fallback_dataset_path = DEFAULT_DATASET_PATH
    dataset_path = Path("data_platform/runtime/ml/test_whale_signal_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_whale_signal_market_snapshot_features.metadata.json")
    analysis_path = Path("data_platform/runtime/ml/test_market_whale_signal_analysis.json")
    export_mode = "fresh_export"
    try:
        with session_scope() as session:
            export_summary = export_market_snapshot_dataset(
                session,
                dataset_path=dataset_path,
                metadata_path=metadata_path,
            )
        row_count, fieldnames, resolution_edges = _dataset_scan(dataset_path)
    except Exception:
        export_mode = "existing_dataset_fallback"
        dataset_path = fallback_dataset_path
        if dataset_path.exists():
            row_count, fieldnames, resolution_edges = _dataset_scan(dataset_path)
        else:
            row_count, fieldnames, resolution_edges = 0, [], []
        export_summary = {"row_count": row_count}
    loaded_rows = _load_training_rows(dataset_path) if dataset_path.exists() else []
    derived_columns_ok = bool(loaded_rows) and all(
        PRICE_BASELINE_COLUMN in row and RESOLUTION_EDGE_COLUMN in row for row in loaded_rows[:3]
    )

    result = analyze_market_whale_signal(
        dataset_path=dataset_path,
        analysis_path=analysis_path,
    )
    summary = result["summary"]
    interpretation = str(summary.get("interpretation") or "").lower()

    checks: list[dict[str, Any]] = [
        {
            "name": "dataset_available",
            "ok": (not args.require_data) or int(export_summary["row_count"]) > 0,
            "dataset_path": str(dataset_path),
            "row_count": row_count,
            "mode": export_mode,
        },
        {
            "name": "derived_columns_present",
            "ok": (PRICE_BASELINE_COLUMN in fieldnames and RESOLUTION_EDGE_COLUMN in fieldnames) or derived_columns_ok,
            "fieldnames": fieldnames,
        },
        {
            "name": "resolution_edge_in_range",
            "ok": all(-1.0 <= value <= 1.0 for value in resolution_edges),
            "min_resolution_edge": min(resolution_edges) if resolution_edges else None,
            "max_resolution_edge": max(resolution_edges) if resolution_edges else None,
        },
        {
            "name": "feature_sets_present",
            "ok": all(name in summary for name in ("price_only", "whale_only", "price_plus_whale")),
        },
        {
            "name": "rolling_metrics_present",
            "ok": summary["price_only"].get("rolling_metrics", {}).get("fold_count", 0) > 0
            and summary["price_plus_whale"].get("rolling_metrics", {}).get("fold_count", 0) > 0,
            "price_only_folds": summary["price_only"].get("rolling_metrics", {}).get("fold_count", 0),
            "price_plus_whale_folds": summary["price_plus_whale"].get("rolling_metrics", {}).get("fold_count", 0),
        },
        {
            "name": "coverage_segment_metrics_present",
            "ok": isinstance(summary["price_plus_whale"].get("coverage_segment_metrics"), dict)
            and "trade_coverage" in summary["price_plus_whale"].get("coverage_segment_metrics", {}),
            "segment_names": list(summary["price_plus_whale"].get("coverage_segment_metrics", {}).keys()),
        },
        {
            "name": "horizon_band_analysis_present",
            "ok": all(
                band in summary.get("horizon_band_analysis", {})
                for band in ("far_168h_plus", "mid_72h_to_167h", "near_under_72h")
            ),
            "band_names": list(summary.get("horizon_band_analysis", {}).keys()),
        },
        {
            "name": "regime_analysis_present",
            "ok": all(
                regime in summary.get("regime_analysis", {})
                for regime in ("trade_covered", "cold_start")
            ),
            "regime_names": list(summary.get("regime_analysis", {}).keys()),
        },
        {
            "name": "whale_gate_uses_trade_covered",
            "ok": summary.get("whale_lift_gate_regime") == "trade_covered",
            "whale_lift_gate_regime": summary.get("whale_lift_gate_regime"),
        },
        {
            "name": "saturation_diagnostics_present",
            "ok": summary.get("price_rule_accuracy") is not None and isinstance(summary.get("price_saturated"), bool),
            "price_rule_accuracy": summary.get("price_rule_accuracy"),
            "price_saturated": summary.get("price_saturated"),
        },
        {
            "name": "price_saturated_interpretation_is_explicit",
            "ok": ("not demonstrated" in interpretation) if bool(summary.get("price_saturated")) else True,
            "interpretation": summary.get("interpretation"),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks, "analysis_path": result["analysis_path"]}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
