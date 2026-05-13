"""Backfill point-in-time whale score snapshots for ML training."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.services.whale_scoring import (
    SCORING_VERSION,
    build_whale_score_snapshot,
    load_source_data_cutoffs,
)
from data_platform.services.storage_lifecycle import create_month_partition


DEFAULT_BACKFILL_SCORING_VERSION = f"{SCORING_VERSION}_point_in_time_backfill"


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _cutoff_range(start: datetime, end: datetime, *, step_hours: int) -> list[datetime]:
    """Return inclusive cutoff timestamps from start to end."""
    if step_hours <= 0:
        raise RuntimeError("--step-hours must be greater than zero.")
    cutoffs: list[datetime] = []
    current = start
    step = timedelta(hours=step_hours)
    while current <= end:
        cutoffs.append(current)
        current += step
    return cutoffs


def _effective_source_cutoff(
    *,
    database_url: str | None,
    requested_cutoff: datetime,
    platform_name: str,
) -> datetime | None:
    """Return the latest source transaction timestamp at or before the requested cutoff."""
    with session_scope(database_url) as session:
        cutoffs = load_source_data_cutoffs(
            session,
            source_data_cutoff=requested_cutoff,
            platform_name=platform_name,
        )
    for details in cutoffs.values():
        if details["platform_name"] == platform_name:
            return details["source_data_cutoff"]
    return None


def _snapshot_exists(
    *,
    database_url: str | None,
    platform_name: str,
    source_data_cutoff: datetime,
) -> bool:
    """Return whether any whale score snapshot already exists for the source cutoff."""
    with session_scope(database_url) as session:
        count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.whale_score_snapshot w
                JOIN analytics.platform p
                  ON p.platform_id = w.platform_id
                WHERE p.platform_name = :platform_name
                  AND w.source_data_cutoff = :source_data_cutoff
                """
            ),
            {"platform_name": platform_name, "source_data_cutoff": source_data_cutoff},
        ).scalar_one()
    return int(count or 0) > 0


def _delete_snapshot_for_cutoff(
    *,
    database_url: str | None,
    platform_name: str,
    source_data_cutoff: datetime,
    scoring_version: str,
) -> dict[str, int]:
    """Delete one backfilled snapshot batch for a cutoff and scoring version."""
    with session_scope(database_url) as session:
        part_deleted = session.execute(
            text(
                """
                DELETE FROM analytics.whale_score_snapshot_part w
                USING analytics.platform p
                WHERE p.platform_id = w.platform_id
                  AND p.platform_name = :platform_name
                  AND w.source_data_cutoff = :source_data_cutoff
                  AND w.scoring_version = :scoring_version
                """
            ),
            {
                "platform_name": platform_name,
                "source_data_cutoff": source_data_cutoff,
                "scoring_version": scoring_version,
            },
        ).rowcount
        legacy_deleted = session.execute(
            text(
                """
                DELETE FROM analytics.whale_score_snapshot w
                USING analytics.platform p
                WHERE p.platform_id = w.platform_id
                  AND p.platform_name = :platform_name
                  AND w.source_data_cutoff = :source_data_cutoff
                  AND w.scoring_version = :scoring_version
                """
            ),
            {
                "platform_name": platform_name,
                "source_data_cutoff": source_data_cutoff,
                "scoring_version": scoring_version,
            },
        ).rowcount
    return {
        "legacy_deleted": int(legacy_deleted or 0),
        "part_deleted": int(part_deleted or 0),
    }


def _backfill_one_cutoff(
    *,
    database_url: str | None,
    requested_cutoff: datetime,
    platform_name: str,
    scoring_version: str,
    replace_existing: bool,
    dry_run: bool,
) -> dict[str, Any]:
    """Backfill one effective point-in-time whale snapshot."""
    effective_cutoff = _effective_source_cutoff(
        database_url=database_url,
        requested_cutoff=requested_cutoff,
        platform_name=platform_name,
    )
    payload: dict[str, Any] = {
        "requested_cutoff": requested_cutoff.isoformat(),
        "effective_source_data_cutoff": effective_cutoff.isoformat() if effective_cutoff else None,
        "platform_name": platform_name,
        "scoring_version": scoring_version,
    }
    if effective_cutoff is None:
        payload["status"] = "skipped_no_source_data"
        return payload

    exists = _snapshot_exists(
        database_url=database_url,
        platform_name=platform_name,
        source_data_cutoff=effective_cutoff,
    )
    if exists and not replace_existing:
        payload["status"] = "skipped_existing"
        return payload
    if dry_run:
        payload["status"] = "dry_run"
        payload["would_replace_existing"] = exists
        return payload
    if exists and replace_existing:
        payload["delete_counts"] = _delete_snapshot_for_cutoff(
            database_url=database_url,
            platform_name=platform_name,
            source_data_cutoff=effective_cutoff,
            scoring_version=scoring_version,
        )

    with session_scope(database_url) as session:
        partition_name = create_month_partition(
            session,
            schema="analytics",
            table_name="whale_score_snapshot_part",
            month_start=effective_cutoff,
        )
        summary = build_whale_score_snapshot(
            session,
            scoring_version=scoring_version,
            source_data_cutoff=effective_cutoff,
            snapshot_time=effective_cutoff,
            platform_name=platform_name,
        )
    payload["status"] = "written"
    payload["partition_name"] = partition_name
    payload["summary"] = summary
    return payload


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Backfill point-in-time whale score snapshots.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--platform-name", default="polymarket", help="Platform to backfill.")
    parser.add_argument(
        "--scoring-version",
        default=DEFAULT_BACKFILL_SCORING_VERSION,
        help="Version label stored with the backfilled snapshot rows.",
    )
    parser.add_argument("--start", required=True, help="Inclusive UTC cutoff start timestamp.")
    parser.add_argument("--end", required=True, help="Inclusive UTC cutoff end timestamp.")
    parser.add_argument("--step-hours", type=int, default=24, help="Hours between requested cutoffs.")
    parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of cutoffs to process.")
    parser.add_argument("--replace-existing", action="store_true", help="Replace existing rows for this scoring version.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned cutoffs without writing rows.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    database_url = args.database_url or None
    cutoffs = _cutoff_range(_parse_datetime(args.start), _parse_datetime(args.end), step_hours=args.step_hours)
    if args.limit > 0:
        cutoffs = cutoffs[: args.limit]

    results = [
        _backfill_one_cutoff(
            database_url=database_url,
            requested_cutoff=cutoff,
            platform_name=args.platform_name,
            scoring_version=args.scoring_version,
            replace_existing=args.replace_existing,
            dry_run=args.dry_run,
        )
        for cutoff in cutoffs
    ]
    status_counts: dict[str, int] = {}
    rows_written = 0
    for result in results:
        status = str(result.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        rows_written += int(result.get("summary", {}).get("rows_written") or 0)
    print(
        json.dumps(
            {
                "cutoff_count": len(cutoffs),
                "status_counts": status_counts,
                "rows_written": rows_written,
                "results": results,
            },
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
