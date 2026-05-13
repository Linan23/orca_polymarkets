"""Dedicated analytics refresh loop for whale scores and dashboard snapshots."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run whale/dashboard refresh on a slower cadence.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--interval-seconds", type=float, default=900.0)
    parser.add_argument("--summary-log-file", default=str(RUNTIME_DIR / "analytics_refresh_runs.jsonl"))
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument(
        "--skip-ml-prediction-snapshots",
        action="store_true",
        help="Skip server-side market-profile ML prediction snapshot generation.",
    )
    parser.add_argument(
        "--ml-prediction-snapshot-limit",
        type=int,
        default=int(os.getenv("ML_PREDICTION_SNAPSHOT_LIMIT", "0")),
        help="Maximum active Polymarket markets to snapshot per analytics refresh cycle. Use 0 for no cap.",
    )
    parser.add_argument(
        "--ml-prediction-snapshot-platform",
        default=os.getenv("ML_PREDICTION_SNAPSHOT_PLATFORM", "polymarket"),
        help="Platform name to snapshot for market-profile ML predictions.",
    )
    return parser.parse_args()


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def main() -> int:
    args = parse_args()
    py = sys.executable
    env = os.environ.copy()
    if args.database_url:
        env["DATABASE_URL"] = args.database_url

    cycle = 0
    while True:
        cycle += 1
        started_at = datetime.now(timezone.utc)
        steps: list[dict[str, object]] = []
        commands: list[tuple[str, list[str]]] = [
            ("refresh_resolved_conditions", [py, "data_platform/jobs/refresh_resolved_conditions.py"]),
            ("build_whale_scores", [py, "data_platform/jobs/build_whale_scores.py"]),
            ("build_dashboard_snapshot", [py, "build_dashboard_snapshot.py"]),
            ("build_home_summary_snapshot", [py, "build_home_summary_snapshot.py"]),
            ("build_research_analytics_snapshot", [py, "build_research_analytics_snapshot.py"]),
        ]
        if not args.skip_ml_prediction_snapshots:
            commands.append(
                (
                    "generate_ml_market_prediction_snapshots",
                    [
                        py,
                        "data_platform/jobs/generate_ml_market_prediction_snapshots.py",
                        "--platform-name",
                        args.ml_prediction_snapshot_platform,
                        "--limit",
                        str(int(args.ml_prediction_snapshot_limit)),
                    ],
                )
            )

        for name, command in commands:
            started = time.monotonic()
            completed = subprocess.run(command, cwd=ROOT_DIR, env=env, text=True, capture_output=True)
            steps.append(
                {
                    "name": name,
                    "ok": completed.returncode == 0,
                    "returncode": completed.returncode,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "stdout_tail": [line for line in completed.stdout.splitlines() if line.strip()][-5:],
                    "stderr_tail": [line for line in completed.stderr.splitlines() if line.strip()][-5:],
                }
            )
            if completed.returncode != 0:
                append_jsonl(Path(args.summary_log_file), {"cycle": cycle, "started_at": started_at.isoformat(), "ok": False, "steps": steps})
                return completed.returncode
        append_jsonl(Path(args.summary_log_file), {"cycle": cycle, "started_at": started_at.isoformat(), "ok": True, "steps": steps})
        if args.max_cycles > 0 and cycle >= args.max_cycles:
            return 0
        time.sleep(max(args.interval_seconds, 0.0))


if __name__ == "__main__":
    raise SystemExit(main())
