"""Validate the market-level ML dataset export."""

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
from data_platform.ml.market_dataset_builder import (
    COVERAGE_FEATURE_COLUMNS,
    FUTURE_MOVEMENT_WINDOWS_HOURS,
    NORMALIZED_TRUSTED_WHALE_PRESSURE_FEATURE_COLUMNS,
    PRICE_BASELINE_COLUMN,
    RECENT_TRUSTED_WHALE_FEATURE_COLUMNS,
    RECENT_WHALE_WINDOWS_HOURS,
    RESOLUTION_EDGE_COLUMN,
    SCORED_WHALE_PRESSURE_FEATURE_COLUMNS,
    STATIC_METADATA_FEATURE_COLUMNS,
    WHALE_FEATURE_COLUMNS,
    export_market_snapshot_dataset,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Validate the market-level ML dataset export.")
    parser.add_argument("--require-data", action="store_true", help="Fail if the exported dataset is empty.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    dataset_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.csv")
    metadata_path = Path("data_platform/runtime/ml/test_resolved_market_snapshot_features.metadata.json")

    with session_scope() as session:
        summary = export_market_snapshot_dataset(
            session,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
        )

    row_count = int(summary["row_count"])
    class_balance = summary["class_balance"]
    horizon_row_counts = summary["horizon_row_counts"]
    with dataset_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        resolution_edges = [float(row.get(RESOLUTION_EDGE_COLUMN, 0) or 0) for row in reader]
    with metadata_path.open(encoding="utf-8") as handle:
        metadata = json.load(handle)
    leakage_audit = metadata.get("leakage_audit", {})
    side_position_target_agreement = leakage_audit.get("side_position_target_agreement")
    movement_columns = [
        f"future_price_delta_{window}h"
        for window in FUTURE_MOVEMENT_WINDOWS_HOURS
    ]
    weighted_whale_columns = [
        "trusted_whale_weighted_score_sum_total",
        "trusted_whale_side_weighted_net_pressure",
        "trusted_whale_side_avg_holding_hours",
        "trusted_whale_side_realized_roi",
    ]
    recent_whale_columns = [
        f"trusted_whale_side_recent_decay_weighted_net_pressure_{window}h"
        for window in RECENT_WHALE_WINDOWS_HOURS
    ]
    normalized_pressure_columns = list(NORMALIZED_TRUSTED_WHALE_PRESSURE_FEATURE_COLUMNS)
    scored_pressure_columns = list(SCORED_WHALE_PRESSURE_FEATURE_COLUMNS)

    checks: list[dict[str, Any]] = [
        {"name": "export_nonempty", "ok": (not args.require_data) or row_count > 0, "row_count": row_count},
        {
            "name": "target_has_both_classes",
            "ok": int(class_balance["side_wins"]) > 0 and int(class_balance["side_loses"]) > 0,
            "class_balance": class_balance,
        },
        {
            "name": "multiple_horizons_present",
            "ok": len([value for value in horizon_row_counts.values() if int(value) > 0]) >= 2,
            "horizon_row_counts": horizon_row_counts,
        },
        {
            "name": "derived_columns_present",
            "ok": PRICE_BASELINE_COLUMN in fieldnames and RESOLUTION_EDGE_COLUMN in fieldnames,
            "fieldnames": fieldnames,
        },
        {
            "name": "coverage_columns_present",
            "ok": all(column in fieldnames for column in COVERAGE_FEATURE_COLUMNS),
            "coverage_columns": list(COVERAGE_FEATURE_COLUMNS),
        },
        {
            "name": "cold_start_metadata_columns_present",
            "ok": all(column in fieldnames for column in STATIC_METADATA_FEATURE_COLUMNS),
            "cold_start_metadata_columns": list(STATIC_METADATA_FEATURE_COLUMNS),
        },
        {
            "name": "resolution_edge_in_range",
            "ok": all(-1.0 <= value <= 1.0 for value in resolution_edges),
            "min_resolution_edge": min(resolution_edges) if resolution_edges else None,
            "max_resolution_edge": max(resolution_edges) if resolution_edges else None,
        },
        {
            "name": "movement_target_columns_present",
            "ok": all(column in fieldnames for column in movement_columns),
            "movement_columns": movement_columns,
        },
        {
            "name": "weighted_whale_behavior_columns_present",
            "ok": all(column in fieldnames for column in weighted_whale_columns)
            and all(column in WHALE_FEATURE_COLUMNS for column in weighted_whale_columns),
            "weighted_whale_columns": weighted_whale_columns,
        },
        {
            "name": "normalized_weighted_pressure_columns_present",
            "ok": all(column in fieldnames for column in normalized_pressure_columns)
            and all(column in WHALE_FEATURE_COLUMNS for column in normalized_pressure_columns),
            "normalized_weighted_pressure_columns": normalized_pressure_columns,
        },
        {
            "name": "scored_whale_pressure_columns_present",
            "ok": all(column in fieldnames for column in scored_pressure_columns)
            and all(column in WHALE_FEATURE_COLUMNS for column in scored_pressure_columns),
            "scored_whale_pressure_columns": scored_pressure_columns,
        },
        {
            "name": "recent_whale_pressure_columns_present",
            "ok": all(column in fieldnames for column in recent_whale_columns)
            and all(column in WHALE_FEATURE_COLUMNS for column in RECENT_TRUSTED_WHALE_FEATURE_COLUMNS),
            "recent_whale_windows_hours": list(RECENT_WHALE_WINDOWS_HOURS),
            "recent_whale_columns": recent_whale_columns,
        },
        {
            "name": "whale_weight_metadata_present",
            "ok": metadata.get("whale_weight_config", {}).get("version") == "whale_weights_v1",
            "whale_weight_config": metadata.get("whale_weight_config"),
        },
        {
            "name": "recent_whale_metadata_present",
            "ok": metadata.get("recent_whale_windows_hours") == list(RECENT_WHALE_WINDOWS_HOURS),
            "recent_whale_windows_hours": metadata.get("recent_whale_windows_hours"),
        },
        {
            "name": "normalized_pressure_metadata_present",
            "ok": metadata.get("normalized_trusted_whale_pressure_columns") == normalized_pressure_columns,
            "normalized_trusted_whale_pressure_columns": metadata.get("normalized_trusted_whale_pressure_columns"),
        },
        {
            "name": "scored_whale_pressure_metadata_present",
            "ok": metadata.get("scored_whale_pressure_columns") == scored_pressure_columns,
            "scored_whale_pressure_columns": metadata.get("scored_whale_pressure_columns"),
        },
        {
            "name": "side_position_not_perfect_target_proxy",
            "ok": side_position_target_agreement is None
            or 0.05 < float(side_position_target_agreement) < 0.95,
            "side_position_target_agreement": side_position_target_agreement,
            "flagged_binary_features": leakage_audit.get("flagged_binary_features"),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
