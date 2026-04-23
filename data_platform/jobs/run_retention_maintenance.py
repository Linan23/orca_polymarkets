"""Nightly maintenance job for partitions, rollups, backfills, and snapshots."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.services.storage_lifecycle import (
    backfill_all_partition_shadows,
    ensure_default_partitions,
    partition_coverage,
    rollup_old_orderbook_snapshots,
    rollup_old_position_snapshots,
)
from data_platform.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run nightly retention/rollup/backup maintenance.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "") or get_settings().database_url)
    parser.add_argument("--rollup-days", type=int, default=30)
    parser.add_argument("--partition-batch-size", type=int, default=5000)
    parser.add_argument("--snapshot-label", default="nightly")
    parser.add_argument("--skip-snapshot", action="store_true")
    parser.add_argument("--summary-log-file", default=str(RUNTIME_DIR / "maintenance_runs.jsonl"))
    return parser.parse_args()


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    with session_scope(args.database_url) as session:
        created_partitions = ensure_default_partitions(session, months_ahead=1)
        backfill_counts = backfill_all_partition_shadows(session, batch_size=args.partition_batch_size)
        orderbook_rollup = rollup_old_orderbook_snapshots(session, older_than_days=args.rollup_days)
        position_rollup = rollup_old_position_snapshots(session, older_than_days=args.rollup_days)
        coverage = partition_coverage(session)

    snapshot_result: dict[str, object] | None = None
    if not args.skip_snapshot:
        env = os.environ.copy()
        env["PSQL_URL"] = args.database_url.replace("+psycopg", "")
        completed = subprocess.run(
            [sys.executable, "scripts/release_snapshot.py", "--label", args.snapshot_label, "--note", "nightly maintenance backup"],
            cwd=ROOT_DIR,
            env=env,
            text=True,
            capture_output=True,
        )
        snapshot_result = {
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout_tail": [line for line in completed.stdout.splitlines() if line.strip()][-5:],
            "stderr_tail": [line for line in completed.stderr.splitlines() if line.strip()][-5:],
        }
        if completed.returncode != 0:
            append_jsonl(
                Path(args.summary_log_file),
                {
                    "started_at": started_at.isoformat(),
                    "ok": False,
                    "created_partitions": created_partitions,
                    "backfill_counts": backfill_counts,
                    "orderbook_rollup": orderbook_rollup,
                    "position_rollup": position_rollup,
                    "coverage": coverage,
                    "snapshot": snapshot_result,
                },
            )
            return completed.returncode

    append_jsonl(
        Path(args.summary_log_file),
        {
            "started_at": started_at.isoformat(),
            "ok": True,
            "created_partitions": created_partitions,
            "backfill_counts": backfill_counts,
            "orderbook_rollup": orderbook_rollup,
            "position_rollup": position_rollup,
            "coverage": coverage,
            "snapshot": snapshot_result,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
