"""Dedicated near-live ingest runner.

This wraps the existing ingestion pipeline with the fast-ingest cadence:
- Polymarket discovery every 10 minutes
- Polymarket public crawl and trades every 2 minutes
- Polymarket orderbooks on a slower configurable cadence
- tracked positions every 10 minutes when wallets are configured
- Kalshi is disabled by default for Polymarket-only deployments
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
from data_platform.services.market_scope import (
    DEFAULT_FOCUS_DOMAINS,
    add_focus_domain_argument,
    canonicalize_focus_domains,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the near-live ingest service loop.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    add_focus_domain_argument(parser)
    parser.add_argument("--timezone", default=os.getenv("LIVE_INGEST_TIMEZONE", "America/New_York"))
    parser.add_argument("--window-start", default=os.getenv("LIVE_INGEST_WINDOW_START", "09:00"))
    parser.add_argument("--window-end", default=os.getenv("LIVE_INGEST_WINDOW_END", "17:00"))
    parser.add_argument("--interval-seconds", type=float, default=120.0)
    parser.add_argument("--jitter-seconds", type=float, default=15.0)
    parser.add_argument("--discovery-every-cycles", type=int, default=5)
    parser.add_argument(
        "--orderbook-every-cycles",
        type=int,
        default=int(os.getenv("LIVE_ORDERBOOK_EVERY_CYCLES", "2")),
        help="Run Polymarket order-book snapshots every N live cycles. Use 0 to disable.",
    )
    parser.add_argument("--positions-every-cycles", type=int, default=5)
    parser.add_argument(
        "--enable-kalshi",
        action="store_true",
        default=os.getenv("LIVE_INGEST_ENABLE_KALSHI", "").lower() in {"1", "true", "yes"},
        help="Enable Kalshi ingest steps. Off by default because the VM is Polymarket-only.",
    )
    parser.add_argument(
        "--failure-cooldown-seconds",
        type=float,
        default=float(os.getenv("LIVE_INGEST_FAILURE_COOLDOWN_SECONDS", "300")),
        help="Cooldown after a failed child ingest cycle, including exhausted rate-limit retries.",
    )
    parser.add_argument(
        "--public-crawl-per-request-delay-seconds",
        type=float,
        default=float(os.getenv("LIVE_PUBLIC_CRAWL_DELAY_SECONDS", "1.0")),
        help="Delay between Polymarket public-crawl trade page requests.",
    )
    parser.add_argument("--polymarket-wallet", action="append", default=[])
    parser.add_argument("--summary-log-file", default=str(RUNTIME_DIR / "ingest_live_runs.jsonl"))
    parser.add_argument("--max-cycles", type=int, default=0)
    args = parser.parse_args()
    try:
        args.focus_domains = canonicalize_focus_domains(args.focus_domain) or list(DEFAULT_FOCUS_DOMAINS)
    except ValueError as exc:
        parser.error(str(exc))
    if args.orderbook_every_cycles < 0:
        parser.error("--orderbook-every-cycles must be >= 0.")
    if args.failure_cooldown_seconds < 0:
        parser.error("--failure-cooldown-seconds must be >= 0.")
    if args.public_crawl_per_request_delay_seconds < 0:
        parser.error("--public-crawl-per-request-delay-seconds must be >= 0.")
    return args


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
        enable_orderbook = args.orderbook_every_cycles > 0 and (
            cycle == 1 or cycle % max(args.orderbook_every_cycles, 1) == 0
        )
        enable_positions = bool(args.polymarket_wallet) and (cycle == 1 or cycle % max(args.positions_every_cycles, 1) == 0)
        started = time.monotonic()
        focus_domain_flags = sum((["--focus-domain", domain] for domain in args.focus_domains), [])
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
            "--public-crawl-per-request-delay-seconds",
            str(args.public_crawl_per_request_delay_seconds),
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
            *focus_domain_flags,
        ]
        if args.database_url:
            cmd.extend(["--database-url", args.database_url])
        if not enable_discovery:
            cmd.append("--skip-discovery")
        if not enable_orderbook:
            cmd.append("--skip-orderbook")
        if not args.enable_kalshi:
            cmd.extend(["--skip-kalshi", "--skip-kalshi-orderbook"])
        if not enable_positions:
            cmd.append("--skip-positions")
        else:
            for wallet in args.polymarket_wallet:
                cmd.extend(["--polymarket-wallet", wallet])
        result = subprocess.run(cmd, cwd=ROOT_DIR)
        if result.returncode != 0:
            if args.max_cycles > 0 and cycle >= args.max_cycles:
                return result.returncode
            cooldown = max(args.failure_cooldown_seconds, 0.0)
            if cooldown > 0:
                print(f"Child ingest cycle failed. Cooling down {cooldown:.0f}s before retrying.")
                time.sleep(cooldown)
            continue
        if args.max_cycles > 0 and cycle >= args.max_cycles:
            return 0
        target_cycle_seconds = max(args.interval_seconds, 0.0) + random.uniform(0.0, max(args.jitter_seconds, 0.0))
        sleep_seconds = max(target_cycle_seconds - (time.monotonic() - started), 0.0)
        if sleep_seconds > 0:
            print(f"Sleeping {sleep_seconds:.2f}s before the next live-ingest cycle.")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
