"""Run one or more end-to-end ingestion cycles for the data platform.

This runner orchestrates the existing entrypoints in a stable order:
1. Bootstrap the database schema.
2. Run Polymarket event discovery.
3. Optionally run the broad Polymarket market/trader crawl.
4. Run Polymarket trades ingestion.
5. Run Polymarket order-book snapshots.
6. Run Polymarket positions for configured wallets.
7. Run Kalshi trades ingestion.
8. Run Kalshi order-book snapshots.
9. Run the optional Dune query ingest.
10. Build preliminary whale scores.
11. Build the derived dashboard snapshot.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.settings import get_settings

DEFAULT_KALSHI_ENVIRONMENT = "prod"
DEFAULT_INTERVAL_SECONDS = 900.0
DEFAULT_TIMEZONE = "America/New_York"
DEFAULT_SUMMARY_LOG_FILE = RUNTIME_DIR / "ingest_cycle_runs.jsonl"


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
    parser.add_argument("--discovery-limit", type=int, default=10, help="Max events requested by the Polymarket discovery step.")
    parser.add_argument(
        "--enable-polymarket-public-crawl",
        action="store_true",
        help="Enable the broad Polymarket market/trader crawl that populates traders from public trades.",
    )
    parser.add_argument(
        "--public-crawl-market-limit",
        type=int,
        default=20,
        help="Number of active Polymarket markets targeted by the broad public crawl.",
    )
    parser.add_argument(
        "--public-crawl-closed-market-limit",
        type=int,
        default=10,
        help="Number of recently closed Polymarket markets targeted by the broad public crawl.",
    )
    parser.add_argument(
        "--public-crawl-closed-within-hours",
        type=float,
        default=None,
        help="Only include closed public-crawl markets within this many hours.",
    )
    parser.add_argument(
        "--public-crawl-closed-within-days",
        type=float,
        default=None,
        help="Only include closed public-crawl markets within this many days.",
    )
    parser.add_argument(
        "--public-crawl-global-pages",
        type=int,
        default=1,
        help="Number of global latest-trade pages fetched by the broad public crawl.",
    )
    parser.add_argument(
        "--public-crawl-max-pages-per-market",
        type=int,
        default=2,
        help="Maximum public trade pages fetched per selected market in the broad public crawl.",
    )
    parser.add_argument(
        "--public-crawl-max-total-trade-pages",
        type=int,
        default=0,
        help="Maximum total public trade pages fetched per broad crawl cycle. 0 means unlimited.",
    )
    parser.add_argument("--polymarket-trades-limit", type=int, default=200, help="Trade row limit for the Polymarket trades step.")
    parser.add_argument("--orderbook-market-limit", type=int, default=10, help="Tracked Polymarket markets sampled in the order-book step.")
    parser.add_argument(
        "--kalshi-environment",
        choices=["demo", "prod"],
        default=DEFAULT_KALSHI_ENVIRONMENT,
        help="Kalshi environment for trades and order-book steps.",
    )
    parser.add_argument("--kalshi-trades-limit", type=int, default=25, help="Trade row limit for the Kalshi trades step.")
    parser.add_argument(
        "--kalshi-orderbook-market-limit",
        type=int,
        default=10,
        help="Tracked Kalshi markets sampled in the order-book step.",
    )
    parser.add_argument("--enable-dune", action="store_true", help="Enable the optional Dune query ingest step.")
    parser.add_argument("--dune-query-id", default=os.getenv("DUNE_QUERY_ID", "2103719"), help="Saved Dune query id.")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Skip the schema bootstrap step.")
    parser.add_argument("--skip-discovery", action="store_true", help="Skip the Polymarket discovery step.")
    parser.add_argument("--skip-polymarket-trades", action="store_true", help="Skip the Polymarket trades step.")
    parser.add_argument("--skip-orderbook", action="store_true", help="Skip the Polymarket order-book snapshot step.")
    parser.add_argument("--skip-positions", action="store_true", help="Skip the Polymarket positions step.")
    parser.add_argument("--skip-kalshi", action="store_true", help="Skip the Kalshi trades step.")
    parser.add_argument("--skip-kalshi-orderbook", action="store_true", help="Skip the Kalshi order-book snapshot step.")
    parser.add_argument("--skip-whale-scores", action="store_true", help="Skip the preliminary whale scoring step.")
    parser.add_argument("--skip-dashboard", action="store_true", help="Skip the derived dashboard snapshot step.")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running later steps even if one step fails.",
    )
    parser.add_argument("--loop", action="store_true", help="Repeat the ingest cycle until interrupted.")
    parser.add_argument("--max-cycles", type=int, default=0, help="When --loop is enabled, stop after this many cycles. 0 means unlimited.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="Sleep duration between cycles when --loop is enabled.",
    )
    parser.add_argument("--interval-minutes", type=float, default=None, help="Alternative to --interval-seconds.")
    parser.add_argument("--interval-hours", type=float, default=None, help="Alternative to --interval-seconds.")
    parser.add_argument("--window-start", default="", help="Optional daily start time in HH:MM (24-hour clock) for allowed crawl window.")
    parser.add_argument("--window-end", default="", help="Optional daily end time in HH:MM (24-hour clock) for allowed crawl window.")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE, help="IANA timezone used for window checks.")
    parser.add_argument("--jitter-seconds", type=float, default=0.0, help="Random delay added after each interval to avoid fixed cadence.")
    parser.add_argument(
        "--summary-log-file",
        default=str(DEFAULT_SUMMARY_LOG_FILE),
        help="JSONL file that stores one compact cycle summary per run. Use an empty string to disable.",
    )
    args = parser.parse_args()

    interval_modes = [
        args.interval_seconds is not None,
        args.interval_minutes is not None,
        args.interval_hours is not None,
    ]
    if sum(interval_modes) > 1:
        parser.error("Use only one of --interval-seconds, --interval-minutes, or --interval-hours.")
    if args.interval_hours is not None:
        args.interval_seconds_effective = args.interval_hours * 3600.0
    elif args.interval_minutes is not None:
        args.interval_seconds_effective = args.interval_minutes * 60.0
    elif args.interval_seconds is not None:
        args.interval_seconds_effective = args.interval_seconds
    else:
        args.interval_seconds_effective = DEFAULT_INTERVAL_SECONDS

    if args.discovery_limit <= 0:
        parser.error("--discovery-limit must be > 0.")
    if args.public_crawl_market_limit <= 0:
        parser.error("--public-crawl-market-limit must be > 0.")
    if args.public_crawl_closed_market_limit < 0:
        parser.error("--public-crawl-closed-market-limit must be >= 0.")
    if args.public_crawl_closed_within_hours is not None and args.public_crawl_closed_within_days is not None:
        parser.error("Use only one of --public-crawl-closed-within-hours or --public-crawl-closed-within-days.")
    if args.public_crawl_closed_within_hours is not None and args.public_crawl_closed_within_hours <= 0:
        parser.error("--public-crawl-closed-within-hours must be > 0.")
    if args.public_crawl_closed_within_days is not None and args.public_crawl_closed_within_days <= 0:
        parser.error("--public-crawl-closed-within-days must be > 0.")
    if args.public_crawl_global_pages < 0:
        parser.error("--public-crawl-global-pages must be >= 0.")
    if args.public_crawl_max_pages_per_market < 0:
        parser.error("--public-crawl-max-pages-per-market must be >= 0.")
    if args.public_crawl_max_total_trade_pages < 0:
        parser.error("--public-crawl-max-total-trade-pages must be >= 0.")
    if args.polymarket_trades_limit <= 0:
        parser.error("--polymarket-trades-limit must be > 0.")
    if args.orderbook_market_limit <= 0:
        parser.error("--orderbook-market-limit must be > 0.")
    if args.kalshi_trades_limit <= 0:
        parser.error("--kalshi-trades-limit must be > 0.")
    if args.kalshi_orderbook_market_limit <= 0:
        parser.error("--kalshi-orderbook-market-limit must be > 0.")
    if args.interval_seconds_effective < 0:
        parser.error("Interval must be >= 0.")
    if bool(args.window_start) != bool(args.window_end):
        parser.error("--window-start and --window-end must be used together.")
    if args.window_start:
        try:
            args.window_start_minutes = parse_clock_time(args.window_start)
            args.window_end_minutes = parse_clock_time(args.window_end)
        except ValueError as exc:
            parser.error(str(exc))
    else:
        args.window_start_minutes = None
        args.window_end_minutes = None
    try:
        args.window_timezone = ZoneInfo(args.timezone)
    except ZoneInfoNotFoundError:
        parser.error(f"Invalid timezone '{args.timezone}'. Use an IANA timezone like America/New_York.")
    if args.jitter_seconds < 0:
        parser.error("--jitter-seconds must be >= 0.")
    if args.max_cycles < 0:
        parser.error("--max-cycles must be >= 0.")
    if args.enable_dune and not args.dune_query_id.strip():
        parser.error("--dune-query-id must not be empty when the Dune step is enabled.")
    args.summary_log_path = Path(args.summary_log_file) if args.summary_log_file.strip() else None

    env_wallets = [wallet.strip() for wallet in os.getenv("POLYMARKET_WALLETS", "").split(",") if wallet.strip()]
    args.polymarket_wallets = args.polymarket_wallet + env_wallets
    return args


def parse_clock_time(value: str) -> int:
    """Parse ``HH:MM`` into minutes since midnight."""
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"Invalid time '{value}'. Use HH:MM in 24-hour format.") from exc
    return parsed.hour * 60 + parsed.minute


def minutes_since_midnight(now: datetime) -> int:
    """Convert a timezone-aware datetime to minutes since local midnight."""
    return now.hour * 60 + now.minute


def is_within_window(now: datetime, start_minutes: int, end_minutes: int) -> bool:
    """Return whether ``now`` falls inside the configured daily window."""
    current = minutes_since_midnight(now)
    if start_minutes == end_minutes:
        return True
    if start_minutes < end_minutes:
        return start_minutes <= current < end_minutes
    return current >= start_minutes or current < end_minutes


def next_window_start(now: datetime, start_minutes: int) -> datetime:
    """Return the next occurrence of the window start after ``now``."""
    start_hour, start_minute = divmod(start_minutes, 60)
    candidate = datetime.combine(now.date(), dt_time(start_hour, start_minute), tzinfo=now.tzinfo)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def wait_for_window(args: argparse.Namespace) -> None:
    """Sleep until the next allowed time window when windowing is enabled."""
    if args.window_start_minutes is None:
        return

    while True:
        now = datetime.now(args.window_timezone)
        if is_within_window(now, args.window_start_minutes, args.window_end_minutes):
            return

        next_start = next_window_start(now, args.window_start_minutes)
        sleep_seconds = max((next_start - now).total_seconds(), 0.0)
        print(
            "Outside active crawler window. "
            f"Sleeping until {next_start.isoformat()} "
            f"({sleep_seconds:.0f}s)."
        )
        time.sleep(sleep_seconds)


def append_summary_log(path: Path | None, summary: dict[str, Any]) -> None:
    """Append one cycle summary to a JSONL runtime log."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(summary, separators=(",", ":")) + "\n")


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
    runtime_dir = RUNTIME_DIR
    discovery_output = runtime_dir / "discovered_events.jsonl"
    polymarket_trades_output = runtime_dir / "polymarket_trades.jsonl"
    positions_output = runtime_dir / "positions.jsonl"
    polymarket_orderbook_output = runtime_dir / "polymarket_orderbook_snapshots.jsonl"
    kalshi_trades_output = runtime_dir / "kalshi_trades.jsonl"
    kalshi_orderbook_output = runtime_dir / "kalshi_orderbook_snapshots.jsonl"
    dune_output = runtime_dir / "dune_query_results.jsonl"

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
                    "--output-file",
                    str(discovery_output),
                ],
            )
        )

    if args.enable_polymarket_public_crawl:
        commands.append(
            (
                "polymarket_market_trader_crawl",
                [
                    py,
                    "data_platform/jobs/polymarket_market_trader_crawl.py",
                    "--skip-refresh-active-events",
                    "--market-limit",
                    str(args.public_crawl_market_limit),
                    "--closed-market-limit",
                    str(args.public_crawl_closed_market_limit),
                    *(
                        ["--closed-within-hours", str(args.public_crawl_closed_within_hours)]
                        if args.public_crawl_closed_within_hours is not None
                        else []
                    ),
                    *(
                        ["--closed-within-days", str(args.public_crawl_closed_within_days)]
                        if args.public_crawl_closed_within_days is not None
                        else []
                    ),
                    "--global-pages",
                    str(args.public_crawl_global_pages),
                    "--trade-limit",
                    str(args.polymarket_trades_limit),
                    "--max-pages-per-market",
                    str(args.public_crawl_max_pages_per_market),
                    "--max-total-trade-pages",
                    str(args.public_crawl_max_total_trade_pages),
                ],
            )
        )

    if not args.skip_polymarket_trades:
        commands.append(
            (
                "polymarket_trades",
                [
                    py,
                    "data_platform/jobs/polymarket_trades_ingest.py",
                    "--limit",
                    str(args.polymarket_trades_limit),
                    "--max-requests",
                    "1",
                    "--output-file",
                    str(polymarket_trades_output),
                ],
            )
        )

    if not args.skip_orderbook:
        commands.append(
            (
                "polymarket_orderbook",
                [
                    py,
                    "data_platform/jobs/polymarket_orderbook_snapshot.py",
                    "--market-limit",
                    str(args.orderbook_market_limit),
                    "--max-requests",
                    "1",
                    "--output-file",
                    str(polymarket_orderbook_output),
                ],
            )
        )

    if not args.skip_positions and args.polymarket_wallets:
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
                        "--output-file",
                        str(positions_output),
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
                    "--output-file",
                    str(kalshi_trades_output),
                ],
            )
        )

    if not args.skip_kalshi_orderbook:
        commands.append(
            (
                "kalshi_orderbook",
                [
                    py,
                    "data_platform/jobs/kalshi_orderbook_snapshot.py",
                    "--environment",
                    args.kalshi_environment,
                    "--market-limit",
                    str(args.kalshi_orderbook_market_limit),
                    "--max-requests",
                    "1",
                    "--output-file",
                    str(kalshi_orderbook_output),
                ],
            )
        )

    if args.enable_dune:
        commands.append(
            (
                "dune_query",
                [
                    py,
                    "data_platform/jobs/dune_query_ingest.py",
                    "--query-id",
                    args.dune_query_id,
                    "--max-requests",
                    "1",
                    "--output-file",
                    str(dune_output),
                ],
            )
        )

    if not args.skip_whale_scores:
        commands.append(("build_whale_scores", [py, "build_whale_scores.py"]))

    if not args.skip_dashboard:
        commands.append(("build_dashboard_snapshot", [py, "build_dashboard_snapshot.py"]))

    return commands


def summarize_cycle(cycle_index: int, results: list[StepResult]) -> dict[str, Any]:
    """Build a compact JSON-serializable summary for one cycle."""
    ok_count = sum(1 for item in results if item.ok)
    failed_count = len(results) - ok_count
    total_duration = sum(item.duration_seconds for item in results)
    return {
        "cycle": cycle_index,
        "ok": failed_count == 0,
        "steps_total": len(results),
        "steps_ok": ok_count,
        "steps_failed": failed_count,
        "cycle_duration_seconds": round(total_duration, 3),
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
    cycle_started_at = datetime.now(timezone.utc)
    database_url = args.database_url or os.getenv("DATABASE_URL", "") or get_settings().database_url
    env = command_env(database_url)
    results: list[StepResult] = []
    for name, command in pipeline_commands(args):
        print(f"Running step: {name}")
        result = run_step(name, command, env)
        results.append(result)
        if not result.ok and not args.continue_on_error:
            break
    summary = summarize_cycle(cycle_index, results)
    summary["started_at"] = cycle_started_at.isoformat()
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    return summary


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    cycle_index = 0
    overall_ok = True
    print(
        "Starting ingest runner: "
        f"interval={args.interval_seconds_effective}s, "
        f"window={args.window_start or 'always'}-{args.window_end or 'always'} {args.timezone}, "
        f"loop={'on' if args.loop else 'off'}, "
        f"max_cycles={args.max_cycles or 'infinite'}, "
        f"summary_log={args.summary_log_path or 'disabled'}"
    )
    if not args.skip_positions and not args.polymarket_wallets:
        print("No Polymarket wallets configured. The positions step will be skipped.")

    while True:
        wait_for_window(args)
        cycle_index += 1
        summary = run_cycle(args, cycle_index)
        print(json.dumps(summary, indent=2))
        append_summary_log(args.summary_log_path, summary)
        overall_ok = overall_ok and summary["ok"]

        if not summary["ok"] and not args.continue_on_error:
            return 1
        if not args.loop:
            return 0 if overall_ok else 1
        if args.max_cycles and cycle_index >= args.max_cycles:
            return 0 if overall_ok else 1

        target_cycle_seconds = args.interval_seconds_effective + random.uniform(0, args.jitter_seconds)
        sleep_seconds = max(target_cycle_seconds - summary["cycle_duration_seconds"], 0.0)
        if sleep_seconds > 0:
            print(f"Sleeping {sleep_seconds:.2f}s before the next cycle.")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
