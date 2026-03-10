"""Week 4/5 readiness gate for data collection and cleaning completeness.

This gate validates that the data platform has:
1. core ingestion coverage for required platforms
2. expected user-activity coverage
3. acceptable scrape freshness
4. passing data-quality checks

Use ``--require-data`` for a strict readiness gate before moving to Week 6.
Without that flag, the script runs in structural mode suitable for CI smoke.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import text

from data_platform.db.session import session_scope
from data_platform.tests.data_quality_check import run_checks as run_data_quality_checks


@dataclass
class ReadinessCheck:
    """One readiness check result."""

    name: str
    ok: bool
    details: dict[str, Any]


REQUIRED_TABLES = {
    "market_event": "analytics.market_event",
    "market_contract": "analytics.market_contract",
    "transaction_fact": "analytics.transaction_fact",
    "orderbook_snapshot": "analytics.orderbook_snapshot",
    "position_snapshot": "analytics.position_snapshot",
    "user_account": "analytics.user_account",
    "scrape_run": "analytics.scrape_run",
}


def _load_platform_ids(session: Any) -> dict[str, int]:
    rows = session.execute(
        text(
            """
            SELECT platform_name, platform_id
            FROM analytics.platform
            """
        )
    ).all()
    return {str(row.platform_name): int(row.platform_id) for row in rows}


def _table_count_for_platform(session: Any, table_name: str, platform_id: int) -> int:
    schema_name, short_name = table_name.split(".", 1)
    return int(
        session.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema_name}"."{short_name}" '  # trusted constant table names only
                "WHERE platform_id = :platform_id"
            ),
            {"platform_id": platform_id},
        ).scalar_one()
    )


def run_checks(require_data: bool, require_dune: bool, max_scrape_age_hours: float) -> list[ReadinessCheck]:
    """Run Week 4/5 readiness checks."""
    results: list[ReadinessCheck] = []
    required_platforms = ["polymarket", "kalshi"] + (["dune"] if require_dune else [])

    with session_scope() as session:
        platform_ids = _load_platform_ids(session)
        missing_platforms = [name for name in required_platforms if name not in platform_ids]
        results.append(
            ReadinessCheck(
                "required_platforms_present",
                (not require_data) or (len(missing_platforms) == 0),
                {
                    "required": required_platforms,
                    "found": sorted(platform_ids.keys()),
                    "missing": missing_platforms,
                    "strict_mode": require_data,
                },
            )
        )

        per_platform_counts: dict[str, dict[str, int]] = {}
        for platform_name in required_platforms:
            platform_id = platform_ids.get(platform_name)
            if platform_id is None:
                per_platform_counts[platform_name] = {key: 0 for key in REQUIRED_TABLES}
                continue
            per_platform_counts[platform_name] = {
                key: _table_count_for_platform(session, table_name, platform_id)
                for key, table_name in REQUIRED_TABLES.items()
            }

        # Week 4 pipeline coverage: markets + trades + orderbook for both platforms.
        for platform_name in required_platforms:
            counts = per_platform_counts[platform_name]
            ok = True
            if require_data:
                ok = (
                    counts["market_event"] > 0
                    and counts["market_contract"] > 0
                    and counts["transaction_fact"] > 0
                    and counts["orderbook_snapshot"] > 0
                    and counts["scrape_run"] > 0
                )
            results.append(
                ReadinessCheck(
                    f"{platform_name}_core_coverage",
                    ok,
                    {
                        "market_event": counts["market_event"],
                        "market_contract": counts["market_contract"],
                        "transaction_fact": counts["transaction_fact"],
                        "orderbook_snapshot": counts["orderbook_snapshot"],
                        "scrape_run": counts["scrape_run"],
                        "strict_mode": require_data,
                    },
                )
            )

        # Week 4 user-activity coverage.
        polymarket_counts = per_platform_counts.get("polymarket", {})
        kalshi_counts = per_platform_counts.get("kalshi", {})
        results.append(
            ReadinessCheck(
                "polymarket_user_activity_coverage",
                (not require_data)
                or (
                    polymarket_counts.get("user_account", 0) > 0
                    and polymarket_counts.get("position_snapshot", 0) > 0
                    and polymarket_counts.get("transaction_fact", 0) > 0
                ),
                {
                    "user_account": polymarket_counts.get("user_account", 0),
                    "position_snapshot": polymarket_counts.get("position_snapshot", 0),
                    "transaction_fact": polymarket_counts.get("transaction_fact", 0),
                    "strict_mode": require_data,
                },
            )
        )
        results.append(
            ReadinessCheck(
                "kalshi_user_activity_coverage",
                (not require_data)
                or (
                    kalshi_counts.get("user_account", 0) > 0
                    and kalshi_counts.get("transaction_fact", 0) > 0
                ),
                {
                    "user_account": kalshi_counts.get("user_account", 0),
                    "transaction_fact": kalshi_counts.get("transaction_fact", 0),
                    "strict_mode": require_data,
                },
            )
        )

        # Freshness gate for strict mode.
        latest_scrape_age_hours = session.execute(
            text(
                """
                SELECT EXTRACT(EPOCH FROM (now() - MAX(COALESCE(finished_at, started_at)))) / 3600.0
                FROM analytics.scrape_run
                """
            )
        ).scalar_one()
        latest_age = float(latest_scrape_age_hours) if latest_scrape_age_hours is not None else None
        results.append(
            ReadinessCheck(
                "ingestion_freshness",
                (not require_data) or (latest_age is not None and latest_age <= max_scrape_age_hours),
                {
                    "latest_scrape_age_hours": latest_age,
                    "max_allowed_hours": max_scrape_age_hours,
                    "strict_mode": require_data,
                },
            )
        )

    # Week 5 cleaning gate: reuse quality checks.
    quality_results = run_data_quality_checks(require_data=require_data)
    failed_quality = [row.name for row in quality_results if not row.ok]
    results.append(
        ReadinessCheck(
            "data_quality_gate",
            len(failed_quality) == 0,
            {
                "failed_checks": failed_quality,
                "passed": len(quality_results) - len(failed_quality),
                "total": len(quality_results),
            },
        )
    )

    return results


def render_text(results: list[ReadinessCheck]) -> str:
    """Render human-readable output."""
    lines: list[str] = []
    for row in results:
        status = "PASS" if row.ok else "FAIL"
        lines.append(f"[{status}] {row.name}")
        for key, value in row.details.items():
            lines.append(f"  {key}: {value}")
    passed = sum(1 for row in results if row.ok)
    lines.append(f"Summary: {passed}/{len(results)} checks passed")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Week 4/5 readiness gate.")
    parser.add_argument(
        "--require-data",
        action="store_true",
        help="Enable strict mode that requires non-empty platform datasets.",
    )
    parser.add_argument(
        "--require-dune",
        action="store_true",
        help="Require Dune ingestion coverage in strict mode.",
    )
    parser.add_argument(
        "--max-scrape-age-hours",
        type=float,
        default=72.0,
        help="Maximum allowed age for latest scrape in strict mode.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args()

    if args.max_scrape_age_hours <= 0:
        parser.error("--max-scrape-age-hours must be > 0.")
    return args


def main() -> int:
    args = parse_args()
    results = run_checks(
        require_data=args.require_data,
        require_dune=args.require_dune,
        max_scrape_age_hours=args.max_scrape_age_hours,
    )
    failed = [row for row in results if not row.ok]

    if args.json:
        print(
            json.dumps(
                {
                    "ok": not failed,
                    "results": [asdict(row) for row in results],
                    "passed": len(results) - len(failed),
                    "failed": len(failed),
                },
                indent=2,
            )
        )
    else:
        print(render_text(results))

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

