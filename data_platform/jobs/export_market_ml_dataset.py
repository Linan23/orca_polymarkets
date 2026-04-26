"""Export the first market-level ML dataset from resolved Polymarket conditions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ml.market_dataset_builder import (
    DEFAULT_DATASET_PATH,
    DEFAULT_HORIZON_HOURS,
    DEFAULT_METADATA_PATH,
    export_market_snapshot_dataset,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Export the point-in-time market-level ML dataset.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV output path for the exported market snapshot feature table.",
    )
    parser.add_argument(
        "--metadata-path",
        default=str(DEFAULT_METADATA_PATH),
        help="JSON metadata output path for the exported market snapshot feature table.",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(value) for value in DEFAULT_HORIZON_HOURS),
        help="Comma-separated hours-before-close horizons, for example 168,24,6,1.",
    )
    parser.add_argument(
        "--whale-weight-config-path",
        default="",
        help="Optional JSON config path for arbitrary whale score weights.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    horizons = tuple(int(part.strip()) for part in args.horizons.split(",") if part.strip())
    with session_scope(args.database_url or None) as session:
        summary = export_market_snapshot_dataset(
            session,
            dataset_path=Path(args.dataset_path),
            metadata_path=Path(args.metadata_path),
            horizon_hours=horizons,
            whale_weight_config_path=Path(args.whale_weight_config_path) if args.whale_weight_config_path else None,
        )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
