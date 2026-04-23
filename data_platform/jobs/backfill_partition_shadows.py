"""One-shot backfill for partition-shadow append-only tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.services.storage_lifecycle import backfill_all_partition_shadows
from data_platform.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill append-only partition shadow tables from legacy tables.")
    parser.add_argument("--database-url", default=get_settings().database_url)
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with session_scope(args.database_url) as session:
        counts = backfill_all_partition_shadows(session, batch_size=args.batch_size)
    print(json.dumps(counts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
