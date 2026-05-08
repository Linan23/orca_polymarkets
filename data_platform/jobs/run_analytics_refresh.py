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
        "--skip-ml-prediction-validation",
        action="store_true",
        help="Skip actual-vs-predicted ML validation scoring.",
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
    parser.add_argument(
        "--ml-prediction-validation-limit",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_LIMIT", "1000")),
        help="Maximum matured ML prediction snapshots to validate per refresh cycle. Use 0 for no cap.",
    )
    parser.add_argument(
        "--ml-prediction-validation-target-tolerance-minutes",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_TARGET_TOLERANCE_MINUTES", "90")),
        help="Nearest orderbook snapshot tolerance around the 12h/24h target time.",
    )
    parser.add_argument(
        "--skip-cache-warm",
        action="store_true",
        help="Skip best-effort warming of dashboard read caches after refresh.",
    )
    parser.add_argument(
        "--cache-warm-api-base-url",
        default=os.getenv("ORCA_API_BASE_URL", "http://127.0.0.1:8001"),
        help="FastAPI base URL used when warming dashboard caches.",
    )
    parser.add_argument(
        "--cache-warm-market-limit",
        type=int,
        default=int(os.getenv("CACHE_WARM_MARKET_LIMIT", "20")),
        help="Number of hot market profile payloads to warm.",
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
        commands: list[tuple[str, list[str], bool]] = [
            ("refresh_resolved_conditions", [py, "refresh_resolved_conditions.py"], True),
            ("build_whale_scores", [py, "build_whale_scores.py"], True),
            ("build_dashboard_snapshot", [py, "build_dashboard_snapshot.py"], True),
            ("build_home_summary_snapshot", [py, "build_home_summary_snapshot.py"], True),
            ("build_research_analytics_snapshot", [py, "build_research_analytics_snapshot.py"], True),
        ]
        if not args.skip_ml_prediction_snapshots and not args.skip_ml_prediction_validation:
            commands.append(
                (
                    "run_ml_prediction_confidence_cycle",
                    [
                        py,
                        "data_platform/jobs/run_ml_prediction_confidence_cycle.py",
                        "--platform-name",
                        args.ml_prediction_snapshot_platform,
                        "--validation-limit",
                        str(int(args.ml_prediction_validation_limit)),
                        "--target-tolerance-minutes",
                        str(int(args.ml_prediction_validation_target_tolerance_minutes)),
                        "--snapshot-limit",
                        str(int(args.ml_prediction_snapshot_limit)),
                    ],
                    True,
                )
            )
        elif not args.skip_ml_prediction_snapshots:
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
                    True,
                )
            )
        if not args.skip_ml_prediction_validation and args.skip_ml_prediction_snapshots:
            commands.append(
                (
                    "validate_ml_market_predictions",
                    [
                        py,
                        "data_platform/jobs/validate_ml_market_predictions.py",
                        "--platform-name",
                        args.ml_prediction_snapshot_platform,
                        "--limit",
                        str(int(args.ml_prediction_validation_limit)),
                        "--target-tolerance-minutes",
                        str(int(args.ml_prediction_validation_target_tolerance_minutes)),
                    ],
                    True,
                )
            )
        if not args.skip_cache_warm:
            commands.append(
                (
                    "warm_dashboard_caches",
                    [
                        py,
                        "data_platform/jobs/warm_dashboard_caches.py",
                        "--api-base-url",
                        args.cache_warm_api_base_url,
                        "--market-limit",
                        str(int(args.cache_warm_market_limit)),
                    ],
                    False,
                )
            )

        for name, command, required in commands:
            started = time.monotonic()
            completed = subprocess.run(command, cwd=ROOT_DIR, env=env, text=True, capture_output=True)
            steps.append(
                {
                    "name": name,
                    "required": required,
                    "ok": completed.returncode == 0,
                    "returncode": completed.returncode,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "stdout_tail": [line for line in completed.stdout.splitlines() if line.strip()][-5:],
                    "stderr_tail": [line for line in completed.stderr.splitlines() if line.strip()][-5:],
                }
            )
            if completed.returncode != 0 and required:
                append_jsonl(Path(args.summary_log_file), {"cycle": cycle, "started_at": started_at.isoformat(), "ok": False, "steps": steps})
                return completed.returncode
        append_jsonl(Path(args.summary_log_file), {"cycle": cycle, "started_at": started_at.isoformat(), "ok": True, "steps": steps})
        if args.max_cycles > 0 and cycle >= args.max_cycles:
            return 0
        time.sleep(max(args.interval_seconds, 0.0))


if __name__ == "__main__":
    raise SystemExit(main())
