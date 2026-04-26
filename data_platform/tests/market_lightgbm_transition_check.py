"""Validate the LightGBM transition gate and benchmark-only Random Forest path."""

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
from data_platform.ml.market_baseline_model import (
    PRICE_FEATURE_COLUMNS,
    PRICE_SATURATION_THRESHOLD,
    TASK_WHALE_SIGNAL,
    _grouped_time_split,
    _load_training_rows,
    _resolve_feature_columns,
    analyze_market_whale_signal,
    compare_market_model_families,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH, export_market_snapshot_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the LightGBM transition gate.")
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
    fallback_dataset_path = DEFAULT_DATASET_PATH
    dataset_path = Path("data_platform/runtime/ml/test_transition_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_transition_market_snapshot_features.metadata.json")
    comparison_path = Path("data_platform/runtime/ml/test_market_model_family_transition.json")
    analysis_path = Path("data_platform/runtime/ml/test_market_whale_signal_analysis.json")
    export_mode = "fresh_export"
    try:
        with session_scope() as session:
            export_summary = export_market_snapshot_dataset(
                session,
                dataset_path=dataset_path,
                metadata_path=metadata_path,
            )
        row_count = int(export_summary["row_count"])
    except Exception:
        export_mode = "existing_dataset_fallback"
        dataset_path = fallback_dataset_path
        row_count = _csv_row_count(dataset_path) if dataset_path.exists() else 0

    rows = _load_training_rows(dataset_path) if dataset_path.exists() else []
    split_ok = False
    split_context: dict[str, Any] = {"train_max_end_time": None, "test_min_end_time": None}
    if rows:
        train_rows, test_rows, _, _ = _grouped_time_split(rows, 0.75)
        split_ok = max(row["market_end_time"] for row in train_rows) < min(row["market_end_time"] for row in test_rows)
        split_context = {
            "train_max_end_time": max(row["market_end_time"] for row in train_rows).isoformat(),
            "test_min_end_time": min(row["market_end_time"] for row in test_rows).isoformat(),
        }

    _, whale_only_columns = _resolve_feature_columns(TASK_WHALE_SIGNAL, "whale_only")
    whale_columns_ok = all(feature not in PRICE_FEATURE_COLUMNS for feature in whale_only_columns)

    comparison_summary = compare_market_model_families(
        dataset_path=dataset_path,
        comparison_path=comparison_path,
    )["summary"]
    analysis_summary = analyze_market_whale_signal(
        dataset_path=dataset_path,
        analysis_path=analysis_path,
    )["summary"]
    transition_gate = comparison_summary.get("transition_gate") or {}

    checks: list[dict[str, Any]] = [
        {
            "name": "dataset_available",
            "ok": (not args.require_data) or row_count > 0,
            "dataset_path": str(dataset_path),
            "row_count": row_count,
            "mode": export_mode,
        },
        {
            "name": "grouped_split_orders_time",
            "ok": split_ok,
            **split_context,
        },
        {
            "name": "whale_only_excludes_price_features",
            "ok": whale_columns_ok,
            "feature_columns": list(whale_only_columns),
        },
        {
            "name": "lightgbm_declared_primary",
            "ok": comparison_summary.get("primary_model") == "lightgbm",
            "primary_model": comparison_summary.get("primary_model"),
        },
        {
            "name": "random_forest_is_benchmark_only",
            "ok": comparison_summary.get("primary_model") == "lightgbm"
            and comparison_summary.get("random_forest", {}).get("estimator_type") == "random_forest",
            "random_forest_estimator": comparison_summary.get("random_forest", {}).get("estimator_type"),
        },
        {
            "name": "rolling_transition_gate_present",
            "ok": transition_gate.get("lightgbm_rolling", {}).get("roc_auc") is not None
            and transition_gate.get("lightgbm_rolling", {}).get("log_loss") is not None
            and transition_gate.get("random_forest_rolling", {}).get("roc_auc") is not None
            and transition_gate.get("random_forest_rolling", {}).get("log_loss") is not None,
            "transition_gate": transition_gate,
        },
        {
            "name": "price_rule_relative_metrics_present",
            "ok": comparison_summary.get("lightgbm_vs_price_rule", {}).get("accuracy_delta") is not None
            and comparison_summary.get("random_forest_vs_price_rule", {}).get("accuracy_delta") is not None,
            "lightgbm_vs_price_rule": comparison_summary.get("lightgbm_vs_price_rule"),
            "random_forest_vs_price_rule": comparison_summary.get("random_forest_vs_price_rule"),
        },
        {
            "name": "transition_regime_analysis_present",
            "ok": all(
                regime in comparison_summary.get("regime_analysis", {})
                for regime in ("trade_covered", "cold_start")
            ),
            "regime_names": list(comparison_summary.get("regime_analysis", {}).keys()),
        },
        {
            "name": "cold_start_regime_uses_cold_start_features",
            "ok": comparison_summary.get("regime_analysis", {}).get("cold_start", {}).get("feature_set") == "cold_start",
            "cold_start_feature_set": comparison_summary.get("regime_analysis", {}).get("cold_start", {}).get("feature_set"),
        },
        {
            "name": "lightgbm_transition_ready",
            "ok": bool(transition_gate.get("lightgbm_ready")),
            "lightgbm_ready": transition_gate.get("lightgbm_ready"),
        },
        {
            "name": "saturation_diagnostic_consistent",
            "ok": (
                comparison_summary.get("price_saturated") is True
                if float(comparison_summary.get("price_rule_accuracy") or 0) > PRICE_SATURATION_THRESHOLD
                else True
            ),
            "price_rule_accuracy": comparison_summary.get("price_rule_accuracy"),
            "price_saturated": comparison_summary.get("price_saturated"),
        },
        {
            "name": "whale_signal_report_present",
            "ok": isinstance(analysis_summary.get("interpretation"), str)
            and "price_plus_whale" in analysis_summary
            and isinstance(analysis_summary.get("whale_lift_demonstrated"), bool)
            and analysis_summary.get("whale_lift_gate_regime") == "trade_covered",
            "interpretation": analysis_summary.get("interpretation"),
            "whale_lift_demonstrated": analysis_summary.get("whale_lift_demonstrated"),
            "whale_lift_gate_regime": analysis_summary.get("whale_lift_gate_regime"),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
