"""Build one preliminary whale score snapshot from normalized source data."""

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
from data_platform.services.whale_scoring import SCORING_VERSION, build_whale_score_snapshot


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Build a preliminary whale score snapshot.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--scoring-version",
        default=SCORING_VERSION,
        help="Version label stored with the snapshot rows.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        summary = build_whale_score_snapshot(session, scoring_version=args.scoring_version)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
