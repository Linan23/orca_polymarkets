"""Export the first model-ready dataset from the normalized PostgreSQL tables."""

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
from data_platform.ml.dataset_builder import DEFAULT_DATASET_PATH, DEFAULT_METADATA_PATH, export_resolved_user_market_dataset


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Export the first ML dataset from resolved Polymarket history.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV output path for the exported feature table.",
    )
    parser.add_argument(
        "--metadata-path",
        default=str(DEFAULT_METADATA_PATH),
        help="JSON metadata output path for the exported feature table.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        summary = export_resolved_user_market_dataset(
            session,
            dataset_path=Path(args.dataset_path),
            metadata_path=Path(args.metadata_path),
        )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
