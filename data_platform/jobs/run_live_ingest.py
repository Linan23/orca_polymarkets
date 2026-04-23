"""Dedicated near-live ingest runner.

This wraps the existing ingestion pipeline with the fast-ingest cadence:
- Polymarket discovery every 10 minutes
- Polymarket public crawl, trades, and orderbooks every 2 minutes
- tracked positions every 10 minutes when wallets are configured
- Kalshi trades and orderbooks every 2 minutes
- no whale/dashboard rebuilds in this loop
"""

from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.jobs.run_ingest_cycle import is_within_window, next_window_start, parse_clock_time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the near-live ingest service loop.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--timezone", default=os.getenv("LIVE_INGEST_TIMEZONE", "America/New_York"))
    parser.add_argument("--window-start", default=os.getenv("LIVE_INGEST_WINDOW_START", "09:00"))
    parser.add_argument("--window-end", default=os.getenv("LIVE_INGEST_WINDOW_END", "17:00"))
    parser.add_argument("--interval-seconds", type=float, default=120.0)
    parser.add_argument("--jitter-seconds", type=float, default=15.0)
    parser.add_argument("--discovery-every-cycles", type=int, default=5)
    parser.add_argument("--positions-every-cycles", type=int, default=5)
    parser.add_argument("--polymarket-wallet", action="append", default=[])
    parser.add_argument("--summary-log-file", default=str(RUNTIME_DIR / "ingest_live_runs.jsonl"))
    parser.add_argument("--max-cycles", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    py = sys.executable
    timezone_obj = ZoneInfo(args.timezone)
    window_start_minutes = parse_clock_time(args.window_start)
    window_end_minutes = parse_clock_time(args.window_end)
    cycle = 0
    while True:
        while True:
            now = datetime.now(timezone_obj)
            if is_within_window(now, window_start_minutes, window_end_minutes):
                break
            next_start = next_window_start(now, window_start_minutes)
            sleep_seconds = max((next_start - now).total_seconds(), 0.0)
            print(
                "Outside live-ingest window. "
                f"Sleeping until {next_start.isoformat()} "
                f"({sleep_seconds:.0f}s)."
            )
            time.sleep(sleep_seconds)

        cycle += 1
        enable_discovery = cycle == 1 or cycle % max(args.discovery_every_cycles, 1) == 0
        enable_positions = bool(args.polymarket_wallet) and (cycle == 1 or cycle % max(args.positions_every_cycles, 1) == 0)
        started = time.monotonic()
        cmd = [
            py,
            "data_platform/jobs/run_ingest_cycle.py",
            "--enable-polymarket-public-crawl",
            "--public-crawl-market-limit",
            "25",
            "--public-crawl-closed-market-limit",
            "10",
            "--public-crawl-closed-within-days",
            "7",
            "--public-crawl-global-pages",
            "2",
            "--public-crawl-max-pages-per-market",
            "3",
            "--public-crawl-max-total-trade-pages",
            "20",
            "--polymarket-trades-limit",
            "200",
            "--orderbook-market-limit",
            "25",
            "--kalshi-trades-limit",
            "25",
            "--kalshi-orderbook-market-limit",
            "10",
            "--skip-whale-scores",
            "--skip-dashboard",
            "--continue-on-error",
            "--window-start",
            args.window_start,
            "--window-end",
            args.window_end,
            "--timezone",
            args.timezone,
            "--interval-seconds",
            str(args.interval_seconds),
            "--jitter-seconds",
            str(args.jitter_seconds),
            "--summary-log-file",
            args.summary_log_file,
        ]
        if args.database_url:
            cmd.extend(["--database-url", args.database_url])
        if not enable_discovery:
            cmd.append("--skip-discovery")
        if not enable_positions:
            cmd.append("--skip-positions")
        else:
            for wallet in args.polymarket_wallet:
                cmd.extend(["--polymarket-wallet", wallet])
        result = subprocess.run(cmd, cwd=ROOT_DIR)
        if result.returncode != 0:
            return result.returncode
        if args.max_cycles > 0 and cycle >= args.max_cycles:
            return 0
        target_cycle_seconds = max(args.interval_seconds, 0.0) + random.uniform(0.0, max(args.jitter_seconds, 0.0))
        sleep_seconds = max(target_cycle_seconds - (time.monotonic() - started), 0.0)
        if sleep_seconds > 0:
            print(f"Sleeping {sleep_seconds:.2f}s before the next live-ingest cycle.")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
