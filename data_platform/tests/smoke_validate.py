"""Lightweight smoke validation for the local data platform stack."""

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

from fastapi.testclient import TestClient
from sqlalchemy import text

from data_platform.api.server import app
from data_platform.db.bootstrap import create_database_objects
from data_platform.db.session import session_scope
from data_platform.services.dashboard_builder import build_dashboard_snapshot


BASELINE_REVISION = "20260304_1200"
REQUIRED_TABLES = {
    "analytics.dashboard",
    "analytics.dashboard_market",
    "analytics.market_contract",
    "analytics.market_event",
    "analytics.market_profile",
    "analytics.market_tag",
    "analytics.market_tag_map",
    "analytics.orderbook_snapshot",
    "analytics.platform",
    "analytics.position_snapshot",
    "analytics.scrape_run",
    "analytics.transaction_fact",
    "analytics.user_account",
    "analytics.user_leaderboard",
    "analytics.user_profile",
    "analytics.whale_score_snapshot",
    "raw.api_payload",
}
CORE_TABLES = (
    "analytics.market_event",
    "analytics.market_contract",
    "analytics.transaction_fact",
    "analytics.position_snapshot",
    "analytics.orderbook_snapshot",
)
API_ENDPOINTS = (
    "/",
    "/health",
    "/api/status/ingestion",
    "/api/markets?limit=1",
    "/api/users?limit=1",
    "/api/transactions?limit=1",
    "/api/positions?limit=1",
    "/api/leaderboards/latest",
    "/api/dashboards/latest",
)


@dataclass
class CheckResult:
    """Represent one smoke-check result."""

    name: str
    ok: bool
    details: dict[str, Any]


def _count_table(session: Any, table_name: str) -> int:
    """Return the row count for a fully qualified table name."""
    schema_name, short_name = table_name.split(".", 1)
    query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{short_name}"')
    return int(session.execute(query).scalar_one())


def _run_database_checks(require_sample_data: bool) -> list[CheckResult]:
    """Run database-level validation checks."""
    results: list[CheckResult] = []
    with session_scope() as session:
        session.execute(text("SELECT 1"))
        results.append(CheckResult("database_connection", True, {"query": "SELECT 1"}))

        revision = session.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        results.append(
            CheckResult(
                "alembic_revision",
                revision == BASELINE_REVISION,
                {"expected": BASELINE_REVISION, "found": revision},
            )
        )

        rows = session.execute(
            text(
                """
                SELECT table_schema || '.' || table_name AS table_name
                FROM information_schema.tables
                WHERE table_schema IN ('analytics', 'raw')
                """
            )
        ).scalars()
        found_tables = set(rows)
        missing_tables = sorted(REQUIRED_TABLES - found_tables)
        results.append(
            CheckResult(
                "required_tables",
                not missing_tables,
                {"required": len(REQUIRED_TABLES), "found": len(found_tables), "missing": missing_tables},
            )
        )

        counts = {table_name: _count_table(session, table_name) for table_name in CORE_TABLES}
        enough_data = all(counts[table_name] > 0 for table_name in CORE_TABLES)
        results.append(
            CheckResult(
                "sample_data",
                (not require_sample_data) or enough_data,
                {"required": require_sample_data, "counts": counts},
            )
        )
    return results


def _run_api_checks() -> list[CheckResult]:
    """Run in-process FastAPI endpoint smoke checks."""
    results: list[CheckResult] = []
    with TestClient(app) as client:
        for endpoint in API_ENDPOINTS:
            response = client.get(endpoint)
            ok = response.status_code == 200
            payload: dict[str, Any]
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_body": response.text[:200]}
            results.append(
                CheckResult(
                    f"api:{endpoint}",
                    ok,
                    {
                        "status_code": response.status_code,
                        "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
                    },
                )
            )
    return results


def _run_optional_checks(run_bootstrap: bool, build_dashboard: bool) -> list[CheckResult]:
    """Run optional mutating checks."""
    results: list[CheckResult] = []

    if run_bootstrap:
        create_database_objects()
        results.append(CheckResult("bootstrap_db_compat", True, {"mode": "create_database_objects"}))

    if build_dashboard:
        with session_scope() as session:
            dashboard = build_dashboard_snapshot(session)
            results.append(
                CheckResult(
                    "dashboard_build",
                    True,
                    {
                        "result": dashboard,
                    },
                )
            )

    return results


def _render_text(results: list[CheckResult]) -> str:
    """Render results in a readable plain-text format."""
    lines: list[str] = []
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        lines.append(f"[{status}] {result.name}")
        for key, value in result.details.items():
            lines.append(f"  {key}: {value}")
    passed = sum(1 for result in results if result.ok)
    lines.append(f"Summary: {passed}/{len(results)} checks passed")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run lightweight smoke validation for the data platform.")
    parser.add_argument(
        "--require-sample-data",
        action="store_true",
        help="Fail if core normalized tables are empty.",
    )
    parser.add_argument(
        "--run-bootstrap",
        action="store_true",
        help="Run the compatibility bootstrap helper before finishing.",
    )
    parser.add_argument(
        "--build-dashboard",
        action="store_true",
        help="Run the dashboard builder as part of validation.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of plain text.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the smoke validator and exit non-zero on failure."""
    args = parse_args()
    results = []
    results.extend(_run_database_checks(require_sample_data=args.require_sample_data))
    results.extend(_run_api_checks())
    results.extend(_run_optional_checks(run_bootstrap=args.run_bootstrap, build_dashboard=args.build_dashboard))

    failed = [result for result in results if not result.ok]
    if args.json:
        print(
            json.dumps(
                {
                    "ok": not failed,
                    "results": [asdict(result) for result in results],
                    "passed": len(results) - len(failed),
                    "failed": len(failed),
                },
                indent=2,
            )
        )
    else:
        print(_render_text(results))

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
