"""Command-line scraper for authenticated Kalshi HTTP endpoints.

This module provides:
1. CLI argument parsing and validation.
2. Auth credential loading.
3. Polling with retry/backoff and optional jitter.
4. Optional JSONL output persistence for scraped records.
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, time as dt_time, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv
from requests.exceptions import HTTPError

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.kalshi import ingest_scrape_record
from clients import KalshiHttpClient, Environment


def resolve_key_file_path(key_file: str) -> Path:
    """Resolve a Kalshi private-key path against repo-local locations when needed."""
    candidate = Path(key_file).expanduser()
    if candidate.is_absolute() and candidate.exists():
        return candidate
    local_candidates = (
        ROOT_DIR / candidate,
        ROOT_DIR / "kalshi-scraper" / candidate.name,
        Path.cwd() / candidate,
    )
    for option in local_candidates:
        if option.exists():
            return option
    return candidate


def parse_query_params(items: list[str]) -> dict[str, str]:
    """Convert repeated CLI query params from ``key=value`` strings to a dictionary.

    Args:
        items: Query parameter entries in ``key=value`` format.

    Returns:
        A dictionary of query parameter keys and values.

    Raises:
        ValueError: If an entry is not in ``key=value`` format or has an empty key.
    """
    params: dict[str, str] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator:
            raise ValueError(f"Invalid --query-param '{item}'. Use key=value.")
        key = key.strip()
        if not key:
            raise ValueError("Invalid --query-param with empty key. Use key=value.")
        params[key] = value
    return params


def parse_args() -> argparse.Namespace:
    """Parse and validate CLI flags for the scraper.

    Returns:
        argparse.Namespace: Parsed and validated CLI arguments. Includes
        ``custom_query_params`` with parsed custom endpoint query parameters.

    Raises:
        SystemExit: Raised by ``argparse`` on invalid CLI usage or validation errors.
    """
    parser = argparse.ArgumentParser(
        description="Poll authenticated Kalshi endpoints with retry, interval control, and JSONL output."
    )
    parser.add_argument("--environment", choices=["demo", "prod"], default="demo")
    parser.add_argument("--endpoint", choices=["balance", "status", "trades", "custom"], default="status")
    parser.add_argument("--interval-seconds", type=float, default=3.0)
    parser.add_argument(
        "--window-start",
        type=str,
        default="",
        help="Optional daily start time in HH:MM (24-hour clock) for allowed scraping window.",
    )
    parser.add_argument(
        "--window-end",
        type=str,
        default="",
        help="Optional daily end time in HH:MM (24-hour clock) for allowed scraping window.",
    )
    parser.add_argument(
        "--timezone",
        type=str,
        default="America/New_York",
        help="IANA timezone used for window checks (default: America/New_York).",
    )
    parser.add_argument("--jitter-seconds", type=float, default=0.5)
    parser.add_argument("--max-requests", type=int, default=0, help="0 means run forever.")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="HTTP timeout per request.")
    parser.add_argument("--output-file", type=str, default="", help="Optional JSONL output path.")
    parser.add_argument(
        "--write-to-db",
        action="store_true",
        help="Also persist scrape results into the configured PostgreSQL database.",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default="",
        help="Optional database URL override. Defaults to DATABASE_URL when omitted.",
    )

    parser.add_argument(
        "--path",
        type=str,
        default="",
        help="Authenticated endpoint path for --endpoint custom (example: /trade-api/v2/portfolio/orders).",
    )
    parser.add_argument(
        "--query-param",
        action="append",
        default=[],
        help="Repeatable custom query parameter in key=value format (for --endpoint custom).",
    )

    # Endpoint-specific trades filters.
    parser.add_argument("--ticker", type=str, default="")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--cursor", type=str, default="")
    parser.add_argument("--max-ts", type=int, default=0)
    parser.add_argument("--min-ts", type=int, default=0)

    args = parser.parse_args()

    if args.endpoint == "custom" and not args.path:
        parser.error("--path is required when --endpoint custom is used.")
    if args.endpoint != "custom" and args.path:
        parser.error("--path can only be used when --endpoint custom is selected.")
    if args.endpoint != "custom" and args.query_param:
        parser.error("--query-param can only be used when --endpoint custom is selected.")
    if args.endpoint == "custom" and "?" in args.path:
        parser.error("Do not include query strings in --path. Use --query-param key=value instead.")
    if bool(args.window_start) != bool(args.window_end):
        parser.error("--window-start and --window-end must be used together.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")
    if args.jitter_seconds < 0:
        parser.error("--jitter-seconds must be >= 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")

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

    try:
        args.custom_query_params = parse_query_params(args.query_param)
    except ValueError as exc:
        parser.error(str(exc))

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


def get_credentials(environment: Environment) -> tuple[str, str]:
    """Load API key metadata for the selected environment from environment variables.

    Args:
        environment: Target Kalshi environment (demo or production).

    Returns:
        tuple[str, str]: ``(key_id, key_file_path)``.

    Raises:
        ValueError: If the required key ID or key file environment variable is missing.
    """
    key_id = os.getenv("DEMO_KEYID") if environment == Environment.DEMO else os.getenv("PROD_KEYID")
    key_file = os.getenv("DEMO_KEYFILE") if environment == Environment.DEMO else os.getenv("PROD_KEYFILE")

    if not key_id:
        raise ValueError("Missing API key ID in environment variables.")
    if not key_file:
        raise ValueError("Missing private key path in environment variables.")

    return key_id, key_file


def load_private_key(key_file: str) -> rsa.RSAPrivateKey:
    """Load an RSA private key from a PEM file.

    Args:
        key_file: Absolute or relative path to a PEM-encoded private key file.

    Returns:
        rsa.RSAPrivateKey: Loaded private key instance.

    Raises:
        FileNotFoundError: If ``key_file`` does not exist.
        Exception: If key parsing fails or file content is invalid.
    """
    resolved_key_file = resolve_key_file_path(key_file)
    try:
        with open(resolved_key_file, "rb") as private_key_file:
            return serialization.load_pem_private_key(
                private_key_file.read(),
                password=None,
            )
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Private key file not found at {resolved_key_file}") from exc
    except Exception as exc:
        raise Exception(f"Error loading private key: {str(exc)}") from exc


def fetch_once(client: KalshiHttpClient, args: argparse.Namespace) -> dict[str, Any]:
    """Execute a single API request based on the selected endpoint mode.

    Args:
        client: Authenticated Kalshi HTTP client.
        args: Parsed CLI arguments determining endpoint and filters.

    Returns:
        dict[str, Any]: JSON response payload from the selected endpoint.

    Raises:
        requests.RequestException: Propagated from underlying HTTP requests.
    """
    if args.endpoint == "balance":
        return client.get_balance(timeout_seconds=args.timeout_seconds)
    if args.endpoint == "status":
        return client.get_exchange_status(timeout_seconds=args.timeout_seconds)
    if args.endpoint == "trades":
        return client.get_trades(
            ticker=args.ticker or None,
            limit=args.limit,
            cursor=args.cursor or None,
            max_ts=args.max_ts or None,
            min_ts=args.min_ts or None,
            timeout_seconds=args.timeout_seconds,
        )

    return client.get_path(
        path=args.path,
        params=args.custom_query_params,
        timeout_seconds=args.timeout_seconds,
    )


def parse_retry_after_seconds(retry_after: str) -> float | None:
    """Parse a ``Retry-After`` header value into seconds.

    Supports both formats:
    1. Integer/float seconds.
    2. HTTP date string.

    Args:
        retry_after: Raw header value from the server.

    Returns:
        float | None: Retry delay in seconds, or ``None`` when unparsable.
    """
    if not retry_after:
        return None
    try:
        return max(float(retry_after), 0.0)
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(retry_after)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)
    except (TypeError, ValueError):
        return None


def compute_retry_delay(attempt: int, args: argparse.Namespace, retry_after_seconds: float | None = None) -> float:
    """Compute retry delay using capped exponential backoff with jitter.

    Args:
        attempt: Zero-based retry attempt number.
        args: Parsed CLI arguments containing backoff configuration.
        retry_after_seconds: Optional server-provided delay from ``Retry-After``.

    Returns:
        float: Delay in seconds before the next retry attempt.
    """
    delay = min(args.backoff_cap_seconds, args.backoff_base_seconds * (2 ** attempt))
    delay += random.uniform(0, 0.25)
    if retry_after_seconds is not None:
        delay = max(delay, retry_after_seconds)
    return delay


def fetch_with_backoff(client: KalshiHttpClient, args: argparse.Namespace) -> dict[str, Any]:
    """Fetch endpoint data with retry handling for transient failures.

    Retry behavior:
    1. Retries HTTP status ``429``, ``500``, ``502``, ``503``, ``504``.
    2. Retries network-level ``requests.RequestException`` errors.
    3. Applies capped exponential backoff plus jitter.
    4. Honors ``Retry-After`` for ``429`` responses when present.

    Args:
        client: Authenticated Kalshi HTTP client.
        args: Parsed CLI arguments including retry and backoff settings.

    Returns:
        dict[str, Any]: JSON payload from a successful request.

    Raises:
        HTTPError: For non-retryable HTTP errors or when max retries are exhausted.
        requests.RequestException: For network errors when max retries are exhausted.
    """
    for attempt in range(args.max_retries + 1):
        try:
            return fetch_once(client, args)
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            retryable = status_code in {429, 500, 502, 503, 504}
            if not retryable or attempt >= args.max_retries:
                raise
            retry_after_seconds = None
            if status_code == 429 and exc.response is not None:
                retry_after_seconds = parse_retry_after_seconds(exc.response.headers.get("Retry-After", ""))
            delay = compute_retry_delay(attempt, args, retry_after_seconds=retry_after_seconds)
            retry_context = "Retry-After respected" if retry_after_seconds is not None else "exponential backoff"
            print(
                f"HTTP {status_code} encountered. Retrying in {delay:.2f}s "
                f"(attempt {attempt + 1}/{args.max_retries}, {retry_context})."
            )
            time.sleep(delay)
        except requests.RequestException as exc:
            if attempt >= args.max_retries:
                raise
            delay = compute_retry_delay(attempt, args)
            print(
                f"Network error ({exc.__class__.__name__}). Retrying in {delay:.2f}s "
                f"(attempt {attempt + 1}/{args.max_retries})."
            )
            time.sleep(delay)


def append_jsonl(output_file: str, record: dict) -> None:
    """Append one scrape record as a JSON line.

    Args:
        output_file: Output path for JSONL records. No file is written when empty.
        record: Serializable scrape record dictionary.

    Returns:
        None

    Raises:
        OSError: If directory creation or file write fails.
        TypeError: If ``record`` is not JSON serializable.
    """
    if not output_file:
        return
    output_path = Path(output_file)
    if output_path.parent and output_path.parent != Path("."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, separators=(",", ":")) + "\n")


def persist_record_to_database(args: argparse.Namespace, record: dict[str, Any]) -> dict[str, int]:
    """Persist one Kalshi scrape record into the normalized database layer."""
    request_url = args.path if args.endpoint == "custom" else args.endpoint
    with session_scope(args.database_url or None) as session:
        return ingest_scrape_record(
            session,
            record=record,
            request_url=request_url,
            raw_output_path=args.output_file,
        )


def main() -> None:
    """Run the CLI scraper loop.

    Steps:
    1. Load environment variables from ``.env``.
    2. Parse and validate CLI arguments.
    3. Initialize authenticated Kalshi client.
    4. Poll the endpoint at configured intervals with retries.
    5. Emit JSON records to stdout and optional JSONL file.

    Returns:
        None

    Raises:
        ValueError: If required API credential environment variables are missing.
        FileNotFoundError: If the configured private key file does not exist.
        Exception: If private key deserialization fails.
        requests.RequestException: If request retries are exhausted.
    """
    load_dotenv()
    args = parse_args()

    environment = Environment.DEMO if args.environment == "demo" else Environment.PROD
    key_id, key_file = get_credentials(environment)
    private_key = load_private_key(key_file)

    client = KalshiHttpClient(
        key_id=key_id,
        private_key=private_key,
        environment=environment,
    )

    print(
        f"Starting scraper: endpoint={args.endpoint}, env={args.environment}, "
        f"interval={args.interval_seconds}s (+jitter {args.jitter_seconds}s), "
        f"window={args.window_start or 'always'}-{args.window_end or 'always'} {args.timezone}, "
        f"max_requests={args.max_requests or 'infinite'}"
    )
    if args.endpoint == "custom":
        print(f"Custom path={args.path}, query_params={args.custom_query_params}")

    request_count = 0
    try:
        while args.max_requests == 0 or request_count < args.max_requests:
            wait_for_window(args)
            started_at = time.time()
            payload = fetch_with_backoff(client, args)
            finished_at = time.time()

            record = {
                "scraped_at_unix": int(started_at),
                "scraped_at_iso": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
                "endpoint": args.endpoint,
                "environment": args.environment,
                "duration_ms": int((finished_at - started_at) * 1000),
                "data": payload,
            }
            if args.endpoint == "custom":
                record["path"] = args.path

            append_jsonl(args.output_file, record)
            print(json.dumps(record, separators=(",", ":")))
            if args.write_to_db:
                try:
                    db_summary = persist_record_to_database(args, record)
                    print(json.dumps({"db_ingest": db_summary}, separators=(",", ":")))
                except Exception as exc:  # pragma: no cover - runtime logging path
                    print(f"Database ingest failed: {exc}")

            request_count += 1
            if args.max_requests and request_count >= args.max_requests:
                break
            target_cycle_seconds = args.interval_seconds + random.uniform(0, args.jitter_seconds)
            sleep_time = max(target_cycle_seconds - (finished_at - started_at), 0)
            time.sleep(max(sleep_time, 0))
    except KeyboardInterrupt:
        print("Scraper stopped by user.")


if __name__ == "__main__":
    main()
