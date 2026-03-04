"""Run one or more end-to-end ingestion cycles for the data platform.

This runner orchestrates the existing entrypoints in a stable order:
1. Bootstrap the database schema.
2. Run Polymarket event discovery.
3. Run Polymarket positions for configured wallets.
4. Run Kalshi trades ingestion.
5. Build the derived dashboard snapshot.

It is intentionally thin and shell command based so the team keeps using the
same entrypoints that are already validated independently.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.settings import get_settings

DEFAULT_KALSHI_ENVIRONMENT = "prod"
DEFAULT_INTERVAL_SECONDS = 900.0


@dataclass
class StepResult:
    """One orchestration step result."""

    name: str
    command: list[str]
    returncode: int
    duration_seconds: float
    ok: bool
    stdout_tail: list[str]
    stderr_tail: list[str]



def parse_args() -> argparse.Namespace:
    """Parse CLI flags for the orchestration runner."""
    parser = argparse.ArgumentParser(description="Run the data platform ingestion pipeline in one command.")
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional database URL override. When omitted, uses DATABASE_URL from the environment.",
    )
    parser.add_argument(
        "--polymarket-wallet",
        action="append",
        default=[],
        help="Repeatable wallet address for the Polymarket positions job. Can also come from POLYMARKET_WALLETS.",
    )
    parser.add_argument(
        "--discovery-limit",
        type=int,
        default=10,
        help="Max events requested by the Polymarket discovery step.",
    )
    parser.add_argument(
        "--kalshi-environment",
        choices=["demo", "prod"],
        default=DEFAULT_KALSHI_ENVIRONMENT,
        help="Kalshi environment for the trades step.",
    )
    parser.add_argument(
        "--kalshi-trades-limit",
        type=int,
        default=25,
        help="Trade row limit for the Kalshi trades step.",
    )
    parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Skip the schema bootstrap step.",
    )
    parser.add_argument(
        "--skip-discovery",
        action="store_true",
        help="Skip the Polymarket discovery step.",
    )
    parser.add_argument(
        "--skip-positions",
        action="store_true",
        help="Skip the Polymarket positions step.",
    )
    parser.add_argument(
        "--skip-kalshi",
        action="store_true",
        help="Skip the Kalshi trades step.",
    )
    parser.add_argument(
        "--skip-dashboard",
        action="store_true",
        help="Skip the derived dashboard snapshot step.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running later steps even if one step fails.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Repeat the ingest cycle until interrupted.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Sleep duration between cycles when --loop is enabled.",
    )
    args = parser.parse_args()

    if args.discovery_limit <= 0:
        parser.error("--discovery-limit must be > 0.")
    if args.kalshi_trades_limit <= 0:
        parser.error("--kalshi-trades-limit must be > 0.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")

    env_wallets = [wallet.strip() for wallet in os.getenv("POLYMARKET_WALLETS", "").split(",") if wallet.strip()]
    args.polymarket_wallets = args.polymarket_wallet + env_wallets
    if args.skip_positions is False and not args.polymarket_wallets:
        parser.error(
            "At least one Polymarket wallet is required for positions. "
            "Use --polymarket-wallet or set POLYMARKET_WALLETS."
        )

    return args

def command_env(database_url: str) -> dict[str, str]:
    """Build the child-process environment for pipeline steps."""
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    return env



def run_step(name: str, command: list[str], env: dict[str, str]) -> StepResult:
    """Run one subprocess step and capture a compact result."""
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=env,
        text=True,
        capture_output=True,
    )
    duration = time.monotonic() - started
    stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
    stderr_lines = [line for line in completed.stderr.splitlines() if line.strip()]
    return StepResult(
        name=name,
        command=command,
        returncode=completed.returncode,
        duration_seconds=duration,
        ok=completed.returncode == 0,
        stdout_tail=stdout_lines[-5:],
        stderr_tail=stderr_lines[-5:],
    )



def pipeline_commands(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    """Build the ordered pipeline command list for one cycle."""
    commands: list[tuple[str, list[str]]] = []
    py = sys.executable

    if not args.skip_bootstrap:
        commands.append(("bootstrap_db", [py, "bootstrap_db.py"]))

    if not args.skip_discovery:
        commands.append(
            (
                "polymarket_discovery",
                [
                    py,
                    "polymarket-data/discover_events_scraper.py",
                    "--fetch-full-details",
                    "--write-to-db",
                    "--max-requests",
                    "1",
                    "--limit",
                    str(args.discovery_limit),
                ],
            )
        )

    if not args.skip_positions:
        for wallet in args.polymarket_wallets:
            commands.append(
                (
                    f"polymarket_positions:{wallet}",
                    [
                        py,
                        "polymarket_positions_scraper.py",
                        "--user-wallet",
                        wallet,
                        "--write-to-db",
                        "--max-requests",
                        "1",
                    ],
                )
            )

    if not args.skip_kalshi:
        commands.append(
            (
                "kalshi_trades",
                [
                    py,
                    "kalshi-scraper/main.py",
                    "--environment",
                    args.kalshi_environment,
                    "--endpoint",
                    "trades",
                    "--write-to-db",
                    "--max-requests",
                    "1",
                    "--limit",
                    str(args.kalshi_trades_limit),
                ],
            )
        )

    if not args.skip_dashboard:
        commands.append(("build_dashboard_snapshot", [py, "build_dashboard_snapshot.py"]))

    return commands



def summarize_cycle(cycle_index: int, results: list[StepResult]) -> dict[str, Any]:
    """Build a compact JSON-serializable summary for one cycle."""
    ok_count = sum(1 for item in results if item.ok)
    failed_count = len(results) - ok_count
    return {
        "cycle": cycle_index,
        "ok": failed_count == 0,
        "steps_total": len(results),
        "steps_ok": ok_count,
        "steps_failed": failed_count,
        "steps": [
            {
                "name": item.name,
                "ok": item.ok,
                "returncode": item.returncode,
                "duration_seconds": round(item.duration_seconds, 3),
                "stdout_tail": item.stdout_tail,
                "stderr_tail": item.stderr_tail,
            }
            for item in results
        ],
    }



def run_cycle(args: argparse.Namespace, cycle_index: int) -> dict[str, Any]:
    """Run one ingest cycle and return a summary."""
    database_url = args.database_url or os.getenv("DATABASE_URL", "") or get_settings().database_url

    env = command_env(database_url)
    results: list[StepResult] = []
    for name, command in pipeline_commands(args):
        print(f"Running step: {name}")
        result = run_step(name, command, env)
        results.append(result)
        if not result.ok and not args.continue_on_error:
            break
    return summarize_cycle(cycle_index, results)



def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    cycle_index = 0

    while True:
        cycle_index += 1
        summary = run_cycle(args, cycle_index)
        print(json.dumps(summary, indent=2))

        if not summary["ok"] and not args.continue_on_error:
            return 1
        if not args.loop:
            return 0 if summary["ok"] else 1

        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
