"""Batch Polymarket event scraper with scheduling and window controls.

This scraper fetches multiple Polymarket events by id in one cycle and writes
one JSONL record per cycle. It supports:
1. Repeated ``--event-id`` flags.
2. Loading event ids from a newline-delimited file.
3. Daily time-window controls.
4. Per-event pacing to reduce bursty API usage.
5. Partial failure handling so one bad id does not stop the whole batch.
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
    """Parse CLI arguments and derive resolved event ids and schedule config."""
    parser = argparse.ArgumentParser(description="Scrape multiple Polymarket events by id on a schedule.")
    parser.add_argument(
        "--event-id",
        action="append",
        default=[],
        help="Repeatable Polymarket event id. Example: --event-id 162522 --event-id 162489",
    )
    parser.add_argument(
        "--event-ids-file",
        default="",
        help="Optional newline-delimited file of event ids. Blank lines and lines starting with # are ignored.",
    )
    parser.add_argument(
        "--output-file",
        default=str(BASE_DIR / "batch_events.jsonl"),
        help="JSONL file to append batch scrape results to.",
    )
    parser.add_argument(
        "--per-event-delay-seconds",
        type=float,
        default=1.0,
        help="Delay between individual event requests within the batch.",
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

    args.event_ids = load_event_ids(args.event_id, args.event_ids_file)

    if args.interval_seconds_effective < 0:
        parser.error("Interval must be >= 0.")
    if not args.event_ids:
        parser.error("Provide at least one --event-id or an --event-ids-file with event ids.")
    if args.per_event_delay_seconds < 0:
        parser.error("--per-event-delay-seconds must be >= 0.")
    if args.jitter_seconds < 0:
        parser.error("--jitter-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")

    return args


def load_event_ids(event_ids: list[str], event_ids_file: str) -> list[str]:
    """Load and de-duplicate event ids from CLI flags and an optional file."""
    resolved: list[str] = []
    seen: set[str] = set()

    for event_id in event_ids:
        event_id = str(event_id).strip()
        if event_id and event_id not in seen:
            seen.add(event_id)
            resolved.append(event_id)

    if event_ids_file:
        file_path = Path(event_ids_file)
        if not file_path.is_file():
            raise argparse.ArgumentTypeError(f"Event ids file not found: {event_ids_file}")
        for line in file_path.read_text(encoding="utf-8").splitlines():
            candidate = line.strip()
            if not candidate or candidate.startswith("#"):
                continue
            if candidate not in seen:
                seen.add(candidate)
                resolved.append(candidate)

    return resolved


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
    """Compute capped exponential backoff with small jitter."""
    delay = min(args.backoff_cap_seconds, args.backoff_base_seconds * (2 ** attempt))
    delay += random.uniform(0, 0.25)
    return delay


def fetch_event_with_backoff(client: httpx.Client, event_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """Fetch one event by id with retry handling for transient failures."""
    for attempt in range(args.max_retries + 1):
        try:
            response = client.get(
                GAMMA_EVENT_URL.format(event_id=event_id),
                timeout=args.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Unexpected response shape: expected an object.")
            return payload
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            retryable = status_code in {429, 500, 502, 503, 504}
            if not retryable or attempt >= args.max_retries:
                raise
            delay = compute_retry_delay(attempt, args)
            print(
                f"HTTP {status_code} for event {event_id}. Retrying in {delay:.2f}s "
                f"(attempt {attempt + 1}/{args.max_retries})."
            )
            time.sleep(delay)
        except httpx.RequestError as exc:
            if attempt >= args.max_retries:
                raise
            delay = compute_retry_delay(attempt, args)
            print(
                f"Network error for event {event_id} ({exc.__class__.__name__}). "
                f"Retrying in {delay:.2f}s (attempt {attempt + 1}/{args.max_retries})."
            )
            time.sleep(delay)

    raise RuntimeError("Unreachable retry loop state.")


def fetch_cycle(client: httpx.Client, args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Fetch all configured events, collecting successes and per-id failures."""
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for index, event_id in enumerate(args.event_ids):
        try:
            event = fetch_event_with_backoff(client, event_id, args)
            results.append(
                {
                    "event_id": str(event_id),
                    "event_slug": event.get("slug"),
                    "title": event.get("title"),
                    "active": event.get("active"),
                    "closed": event.get("closed"),
                    "markets_count": len(event.get("markets", [])) if isinstance(event.get("markets"), list) else None,
                    "data": event,
                }
            )
        except Exception as exc:  # pragma: no cover - broad by design for per-item isolation
            errors.append({"event_id": str(event_id), "error": str(exc)})

        if index < len(args.event_ids) - 1 and args.per_event_delay_seconds > 0:
            time.sleep(args.per_event_delay_seconds + random.uniform(0, min(args.jitter_seconds, 0.5)))

    return results, errors


def append_jsonl(output_file: str, record: dict[str, Any]) -> None:
    """Append one JSON record to a JSONL file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, separators=(",", ":")) + "\n")


def main() -> None:
    """Run scheduled batch scraping cycles."""
    args = parse_args()
    print(
        "Starting Polymarket batch event scraper: "
        f"event_ids={len(args.event_ids)}, interval={args.interval_seconds_effective}s, "
        f"window={args.window_start or 'always'}-{args.window_end or 'always'} {args.timezone}, "
        f"max_requests={args.max_requests or 'infinite'}"
    )

    request_count = 0
    with httpx.Client() as client:
        while args.max_requests == 0 or request_count < args.max_requests:
            wait_for_window(args)
            started_at = time.time()
            results, errors = fetch_cycle(client, args)
            finished_at = time.time()

            record = {
                "scraped_at_unix": int(started_at),
                "scraped_at_iso": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "event_ids": args.event_ids,
                "requested_count": len(args.event_ids),
                "results_count": len(results),
                "errors_count": len(errors),
                "results": results,
                "errors": errors,
            }
            append_jsonl(args.output_file, record)
            print(
                json.dumps(
                    {
                        "scraped_at": record["scraped_at_iso"],
                        "requested_count": record["requested_count"],
                        "results_count": record["results_count"],
                        "errors_count": record["errors_count"],
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
