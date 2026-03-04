"""Periodic scraper for Polymarket's event-by-id endpoint.

The scraper fetches a single event from the Polymarket Gamma API and appends
each response as one JSON line to an output file.
"""

import argparse
import json
import random
import time
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

BASE_DIR = Path(__file__).resolve().parent
GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/{event_id}"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments and derive the effective polling interval."""
    parser = argparse.ArgumentParser(description="Scrape a Polymarket event by id on a schedule.")
    parser.add_argument("--event-id", required=True, help="Polymarket event id, for example 162522.")
    parser.add_argument(
        "--output-file",
        default=str(BASE_DIR / "event_by_id.jsonl"),
        help="JSONL file to append scrape results to.",
    )
    parser.add_argument("--interval-seconds", type=float, default=None)
    parser.add_argument("--interval-minutes", type=float, default=None)
    parser.add_argument("--interval-hours", type=float, default=None)
    parser.add_argument(
        "--window-start",
        default="",
        help="Optional daily start time in HH:MM (24-hour clock) for allowed scraping window.",
    )
    parser.add_argument(
        "--window-end",
        default="",
        help="Optional daily end time in HH:MM (24-hour clock) for allowed scraping window.",
    )
    parser.add_argument(
        "--timezone",
        default="America/New_York",
        help="IANA timezone used for window checks (default: America/New_York).",
    )
    parser.add_argument("--jitter-seconds", type=float, default=0.0)
    parser.add_argument("--max-requests", type=int, default=0, help="0 means run forever.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)

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
        args.interval_seconds_effective = 3600.0

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

    if args.interval_seconds_effective < 0:
        parser.error("Interval must be >= 0.")
    if args.jitter_seconds < 0:
        parser.error("--jitter-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")

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
        sleep_seconds = max((next_start - now).total_seconds(), 0)
        print(
            "Outside active window. "
            f"Sleeping until {next_start.isoformat()} "
            f"({sleep_seconds:.0f}s)."
        )
        time.sleep(sleep_seconds)


def compute_retry_delay(attempt: int, args: argparse.Namespace) -> float:
    """Compute exponential backoff delay with a small random jitter."""
    delay = min(args.backoff_cap_seconds, args.backoff_base_seconds * (2 ** attempt))
    delay += random.uniform(0, 0.25)
    return delay


def fetch_once(client: httpx.Client, args: argparse.Namespace) -> dict[str, Any]:
    """Fetch one event by id from the Gamma API."""
    response = client.get(
        GAMMA_EVENT_URL.format(event_id=args.event_id),
        timeout=args.timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response shape: expected an object.")
    return payload


def fetch_with_backoff(client: httpx.Client, args: argparse.Namespace) -> dict[str, Any]:
    """Fetch an event with retries for transient HTTP and network failures."""
    for attempt in range(args.max_retries + 1):
        try:
            return fetch_once(client, args)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            retryable = status_code in {429, 500, 502, 503, 504}
            if not retryable or attempt >= args.max_retries:
                raise
            delay = compute_retry_delay(attempt, args)
            print(
                f"HTTP {status_code} encountered. Retrying in {delay:.2f}s "
                f"(attempt {attempt + 1}/{args.max_retries})."
            )
            time.sleep(delay)
        except httpx.RequestError as exc:
            if attempt >= args.max_retries:
                raise
            delay = compute_retry_delay(attempt, args)
            print(
                f"Network error ({exc.__class__.__name__}). Retrying in {delay:.2f}s "
                f"(attempt {attempt + 1}/{args.max_retries})."
            )
            time.sleep(delay)

    raise RuntimeError("Unreachable retry loop state.")


def append_jsonl(output_file: str, record: dict[str, Any]) -> None:
    """Append one JSON-serializable record to a JSONL file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, separators=(",", ":")) + "\n")


def main() -> None:
    """Run the scheduled event-by-id scraping loop."""
    args = parse_args()
    print(
        "Starting Polymarket event scraper: "
        f"event_id={args.event_id}, interval={args.interval_seconds_effective}s, "
        f"window={args.window_start or 'always'}-{args.window_end or 'always'} {args.timezone}, "
        f"max_requests={args.max_requests or 'infinite'}"
    )

    request_count = 0
    with httpx.Client() as client:
        while args.max_requests == 0 or request_count < args.max_requests:
            wait_for_window(args)
            started_at = time.time()
            event = fetch_with_backoff(client, args)
            finished_at = time.time()

            record = {
                "scraped_at_unix": int(started_at),
                "scraped_at_iso": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "event_id": args.event_id,
                "event_slug": event.get("slug"),
                "title": event.get("title"),
                "active": event.get("active"),
                "closed": event.get("closed"),
                "markets_count": len(event.get("markets", [])) if isinstance(event.get("markets"), list) else None,
                "tags_count": len(event.get("tags", [])) if isinstance(event.get("tags"), list) else None,
                "data": event,
            }
            append_jsonl(args.output_file, record)
            print(
                json.dumps(
                    {
                        "scraped_at": record["scraped_at_iso"],
                        "event_id": record["event_id"],
                        "slug": record["event_slug"],
                        "markets_count": record["markets_count"],
                    }
                )
            )

            request_count += 1
            if args.max_requests and request_count >= args.max_requests:
                break

            target_cycle_seconds = args.interval_seconds_effective + random.uniform(0, args.jitter_seconds)
            sleep_seconds = max(target_cycle_seconds - (finished_at - started_at), 0)
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
