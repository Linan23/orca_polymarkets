"""Refresh persisted resolved Polymarket condition outcomes."""

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
from data_platform.services.resolved_conditions import refresh_resolved_conditions


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Refresh persisted resolved Polymarket condition outcomes.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--require-data",
        action="store_true",
        help="Exit non-zero when no resolved conditions are available after refresh.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        summary = refresh_resolved_conditions(session)
    print(json.dumps(summary, sort_keys=True))
    if args.require_data and summary["resolved_conditions"] <= 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
