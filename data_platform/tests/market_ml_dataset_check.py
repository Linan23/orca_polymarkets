"""Validate the market-level ML dataset export."""

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
from data_platform.ml.market_dataset_builder import export_market_snapshot_dataset


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
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
