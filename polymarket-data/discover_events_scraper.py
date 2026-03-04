"""Dynamic Polymarket event scraper.

This scraper does not require fixed event ids ahead of time. It:
1. Discovers events from the Polymarket Gamma API `/events` list endpoint.
2. Optionally filters them locally by slug/title text.
3. Optionally fetches full event-by-id payloads for each discovered event.
4. Writes one JSONL record per scrape cycle.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.polymarket import ingest_discovery_cycle

EVENTS_LIST_URL = "https://gamma-api.polymarket.com/events"
EVENT_BY_ID_URL = "https://gamma-api.polymarket.com/events/{event_id}"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments and derive the effective cycle interval."""
    parser = argparse.ArgumentParser(description="Discover and scrape Polymarket events on a schedule.")
    parser.add_argument(
        "--output-file",
        default=str(BASE_DIR / "discovered_events.jsonl"),
        help="JSONL file to append scrape cycles to.",
    )
    parser.add_argument(
        "--write-to-db",
        action="store_true",
        help="Also persist scrape results into the configured PostgreSQL database.",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional database URL override. Defaults to DATABASE_URL when omitted.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Max events to request from the list endpoint.")
    parser.add_argument(
        "--active",
        choices=["true", "false", "any"],
        default="true",
        help="Filter list request by active state; default is true.",
    )
    parser.add_argument(
        "--closed",
        choices=["true", "false", "any"],
        default="false",
        help="Filter list request by closed state; default is false.",
    )
    parser.add_argument(
        "--query-text",
        default="",
        help="Optional case-insensitive text filter applied to event title and slug after discovery.",
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Repeatable tag filter matched against event tag labels and slugs (case-insensitive).",
    )
    parser.add_argument(
        "--tag-mode",
        choices=["any", "all"],
        default="any",
        help="How multiple --tag filters are applied: any match or require all matches.",
    )
    parser.add_argument(
        "--fetch-full-details",
        action="store_true",
        help="After discovery, fetch each event again via /events/{id} for full canonical detail.",
    )
    parser.add_argument(
        "--per-event-delay-seconds",
        type=float,
        default=1.0,
        help="Delay between event-by-id detail requests when --fetch-full-details is enabled.",
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
    except ZoneInfoNotFoundError as exc:
        parser.error(f"Invalid timezone '{args.timezone}'. Use an IANA timezone like America/New_York.")

    if args.interval_seconds_effective < 0:
        parser.error("Interval must be >= 0.")
    if args.limit <= 0:
        parser.error("--limit must be > 0.")
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


def request_with_backoff(
    client: httpx.Client,
    url: str,
    args: argparse.Namespace,
    params: dict[str, Any] | None = None,
) -> Any:
    """Issue one GET request with retry handling for transient errors."""
    for attempt in range(args.max_retries + 1):
        try:
            response = client.get(url, params=params, timeout=args.timeout_seconds)
            response.raise_for_status()
            return response.json()
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


def build_list_params(args: argparse.Namespace) -> dict[str, Any]:
    """Build request params for the events list endpoint."""
    params: dict[str, Any] = {"limit": args.limit}
    if args.active != "any":
        params["active"] = args.active
    if args.closed != "any":
        params["closed"] = args.closed
    return params


def event_matches_text(event: dict[str, Any], query_text: str) -> bool:
    """Return whether an event matches the optional title/slug text filter."""
    if not query_text:
        return True
    needle = query_text.casefold()
    haystacks = [
        str(event.get("title", "")).casefold(),
        str(event.get("slug", "")).casefold(),
    ]
    return any(needle in hay for hay in haystacks)


def event_matches_tags(event: dict[str, Any], tags: list[str], tag_mode: str) -> bool:
    """Return whether an event's tag labels/slugs satisfy the configured tag filters."""
    if not tags:
        return True

    event_tags: set[str] = set()
    raw_tags = event.get("tags")
    if isinstance(raw_tags, list):
        for tag in raw_tags:
            if not isinstance(tag, dict):
                continue
            label = str(tag.get("label", "")).strip()
            slug = str(tag.get("slug", "")).strip()
            if label:
                event_tags.add(label.casefold())
            if slug:
                event_tags.add(slug.casefold())

    wanted = [tag.strip().casefold() for tag in tags if tag.strip()]
    if not wanted:
        return True
    if tag_mode == "all":
        return all(tag in event_tags for tag in wanted)
    return any(tag in event_tags for tag in wanted)


def filter_events_with_tags(
    events: list[dict[str, Any]],
    query_text: str,
    tags: list[str],
    tag_mode: str,
) -> list[dict[str, Any]]:
    """Apply local substring and tag filtering on discovered events."""
    filtered: list[dict[str, Any]] = []
    for event in events:
        if event_matches_text(event, query_text) and event_matches_tags(event, tags, tag_mode):
            filtered.append(event)
    return filtered


def fetch_cycle(client: httpx.Client, args: argparse.Namespace) -> dict[str, Any]:
    """Run one discovery cycle and return structured results."""
    discovered = request_with_backoff(client, EVENTS_LIST_URL, args, params=build_list_params(args))
    if not isinstance(discovered, list):
        raise ValueError("Unexpected response shape from /events: expected a list.")

    filtered = filter_events_with_tags(
        discovered,
        query_text=args.query_text,
        tags=args.tag,
        tag_mode=args.tag_mode,
    )
    errors: list[dict[str, str]] = []

    if not args.fetch_full_details:
        return {
            "discovered_count": len(discovered),
            "matched_count": len(filtered),
            "results": filtered,
            "errors": errors,
        }

    detailed_results: list[dict[str, Any]] = []
    for index, event in enumerate(filtered):
        event_id = event.get("id")
        if event_id is None:
            errors.append({"event_id": "", "error": "Missing event id in discovery payload."})
            continue
        try:
            detail = request_with_backoff(client, EVENT_BY_ID_URL.format(event_id=event_id), args)
            if not isinstance(detail, dict):
                raise ValueError("Unexpected response shape from /events/{id}: expected an object.")
            detailed_results.append(detail)
        except Exception as exc:  # pragma: no cover - broad by design for per-item isolation
            errors.append({"event_id": str(event_id), "error": str(exc)})

        if index < len(filtered) - 1 and args.per_event_delay_seconds > 0:
            time.sleep(args.per_event_delay_seconds + random.uniform(0, min(args.jitter_seconds, 0.5)))

    return {
        "discovered_count": len(discovered),
        "matched_count": len(filtered),
        "results": detailed_results,
        "errors": errors,
    }


def append_jsonl(output_file: str, record: dict[str, Any]) -> None:
    """Append one JSON record to a JSONL file."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, separators=(",", ":")) + "\n")


def persist_cycle_to_database(args: argparse.Namespace, cycle: dict[str, Any]) -> dict[str, int]:
    """Persist one discovery cycle into the normalized database layer."""
    params = build_list_params(args)
    query_string = urlencode(params)
    request_url = f"{EVENTS_LIST_URL}?{query_string}" if query_string else EVENTS_LIST_URL
    with session_scope(args.database_url or None) as session:
        return ingest_discovery_cycle(
            session,
            cycle=cycle,
            request_url=request_url,
            raw_output_path=args.output_file,
        )


def main() -> None:
    """Run scheduled discovery/scrape cycles."""
    args = parse_args()
    print(
        "Starting Polymarket discovery scraper: "
        f"limit={args.limit}, active={args.active}, closed={args.closed}, "
        f"fetch_full_details={args.fetch_full_details}, "
        f"interval={args.interval_seconds_effective}s, "
        f"window={args.window_start or 'always'}-{args.window_end or 'always'} {args.timezone}, "
        f"max_requests={args.max_requests or 'infinite'}"
    )

    request_count = 0
    with httpx.Client() as client:
        while args.max_requests == 0 or request_count < args.max_requests:
            wait_for_window(args)
            started_at = time.time()
            cycle = fetch_cycle(client, args)
            finished_at = time.time()

            results = cycle["results"]
            record = {
                "scraped_at_unix": int(started_at),
                "scraped_at_iso": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "discovered_count": cycle["discovered_count"],
                "matched_count": cycle["matched_count"],
                "results_count": len(results) if isinstance(results, list) else None,
                "errors_count": len(cycle["errors"]),
                "results": results,
                "errors": cycle["errors"],
            }
            append_jsonl(args.output_file, record)
            print(
                json.dumps(
                    {
                        "scraped_at": record["scraped_at_iso"],
                        "discovered_count": record["discovered_count"],
                        "matched_count": record["matched_count"],
                        "results_count": record["results_count"],
                        "errors_count": record["errors_count"],
                    }
                )
            )
            if args.write_to_db:
                try:
                    db_summary = persist_cycle_to_database(args, cycle)
                    print(json.dumps({"db_ingest": db_summary}))
                except Exception as exc:  # pragma: no cover - defensive runtime logging
                    print(f"Database ingest failed: {exc}")

            request_count += 1
            if args.max_requests and request_count >= args.max_requests:
                break

            target_cycle_seconds = args.interval_seconds_effective + random.uniform(0, args.jitter_seconds)
            sleep_seconds = max(target_cycle_seconds - (finished_at - started_at), 0)
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
