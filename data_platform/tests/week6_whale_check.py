"""Validate the preliminary whale scoring and dashboard outputs."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.api.server import app
from data_platform.db.session import session_scope
from data_platform.ingest.store import UNKNOWN_USER_EXTERNAL_REF
from data_platform.services.dashboard_builder import build_dashboard_snapshot
from data_platform.services.whale_scoring import build_whale_score_snapshot


@dataclass
class CheckResult:
    """Represent one validation result."""

    name: str
    ok: bool
    details: dict[str, Any]


def _scalar(session: Any, query: str, params: dict[str, Any] | None = None) -> int:
    """Return an integer scalar from a text query."""
    value = session.execute(text(query), params or {}).scalar_one()
    return int(value or 0)


def _run_checks(require_data: bool) -> list[CheckResult]:
    """Run Week 6 whale analytics checks."""
    latest_dashboard_id: int | None = None
    latest_public_raw_count = 0
    latest_trusted_count = 0
    latest_market_row_count = 0
    latest_market_fields_count = 0
    whale_count = 0
    trusted_whale_count = 0
    latest_scoring_version: str | None = None

    with session_scope() as session:
        source_user_count = _scalar(
            session,
            """
            SELECT COUNT(DISTINCT tf.user_id)
            FROM analytics.transaction_fact tf
            JOIN analytics.user_account ua
              ON ua.user_id = tf.user_id
            WHERE ua.external_user_ref <> :unknown_user_ref
            """,
            {"unknown_user_ref": UNKNOWN_USER_EXTERNAL_REF},
        )
        latest_score_user_count = _scalar(
            session,
            """
            WITH latest_batch AS (
              SELECT w.snapshot_time, w.scoring_version
              FROM analytics.whale_score_snapshot w
              ORDER BY w.snapshot_time DESC, w.whale_score_snapshot_id DESC
              LIMIT 1
            ),
            latest_scores AS (
              SELECT w.user_id
              FROM analytics.whale_score_snapshot w
              JOIN latest_batch lb
                ON lb.snapshot_time = w.snapshot_time
               AND lb.scoring_version = w.scoring_version
            )
            SELECT COUNT(*) FROM latest_scores
            """,
        )
        latest_dashboard_id = session.execute(
            text("SELECT dashboard_id FROM analytics.dashboard ORDER BY dashboard_id DESC LIMIT 1")
        ).scalar_one_or_none()
        latest_scoring_version = session.execute(
            text(
                """
                SELECT scoring_version
                FROM analytics.whale_score_snapshot
                ORDER BY snapshot_time DESC, whale_score_snapshot_id DESC
                LIMIT 1
                """
            )
        ).scalar_one_or_none()
        whale_count = _scalar(
            session,
            """
            WITH latest_batch AS (
              SELECT w.snapshot_time, w.scoring_version
              FROM analytics.whale_score_snapshot w
              ORDER BY w.snapshot_time DESC, w.whale_score_snapshot_id DESC
              LIMIT 1
            )
            SELECT COUNT(*)
            FROM analytics.whale_score_snapshot w
            JOIN latest_batch lb
              ON lb.snapshot_time = w.snapshot_time
             AND lb.scoring_version = w.scoring_version
            WHERE w.is_whale = TRUE
            """,
        )
        trusted_whale_count = _scalar(
            session,
            """
            WITH latest_batch AS (
              SELECT w.snapshot_time, w.scoring_version
              FROM analytics.whale_score_snapshot w
              ORDER BY w.snapshot_time DESC, w.whale_score_snapshot_id DESC
              LIMIT 1
            )
            SELECT COUNT(*)
            FROM analytics.whale_score_snapshot w
            JOIN latest_batch lb
              ON lb.snapshot_time = w.snapshot_time
             AND lb.scoring_version = w.scoring_version
            WHERE w.is_trusted_whale = TRUE
            """,
        )
        if latest_dashboard_id is not None:
            latest_public_raw_count = _scalar(
                session,
                """
                SELECT COUNT(*)
                FROM analytics.user_leaderboard
                WHERE dashboard_id = :dashboard_id
                  AND board_type = 'public_raw'
                """,
                {"dashboard_id": latest_dashboard_id},
            )
            latest_trusted_count = _scalar(
                session,
                """
                SELECT COUNT(*)
                FROM analytics.user_leaderboard
                WHERE dashboard_id = :dashboard_id
                  AND board_type = 'internal_trusted'
                """,
                {"dashboard_id": latest_dashboard_id},
            )
            latest_market_row_count = _scalar(
                session,
                """
                SELECT COUNT(*)
                FROM analytics.dashboard_market
                WHERE dashboard_id = :dashboard_id
                """,
                {"dashboard_id": latest_dashboard_id},
            )
            latest_market_fields_count = _scalar(
                session,
                """
                SELECT COUNT(*)
                FROM analytics.dashboard_market
                WHERE dashboard_id = :dashboard_id
                  AND whale_count IS NOT NULL
                  AND trusted_whale_count IS NOT NULL
                """,
                {"dashboard_id": latest_dashboard_id},
            )

    api_results: dict[str, Any] = {
        "home_summary_status": None,
        "home_summary_keys": [],
        "leaderboard_status": None,
        "leaderboard_rows": 0,
        "leaderboard_row_keys": [],
        "top_trusted_whale_present": False,
        "top_trusted_whale_keys": [],
        "most_whale_market_present": False,
        "most_whale_market_keys": [],
        "summary_whales_detected": None,
        "summary_trusted_whales": None,
        "summary_scoring_version": None,
    }
    with TestClient(app) as client:
        home_response = client.get("/api/home/summary")
        api_results["home_summary_status"] = home_response.status_code
        if home_response.status_code == 200:
            home_payload = home_response.json()
            summary = (home_payload or {}).get("summary") or {}
            api_results["home_summary_keys"] = sorted(summary.keys())
            api_results["summary_whales_detected"] = summary.get("whales_detected")
            api_results["summary_trusted_whales"] = summary.get("trusted_whales")
            api_results["summary_scoring_version"] = summary.get("scoring_version")
            top_trusted = summary.get("top_trusted_whale")
            most_whale_market = summary.get("most_whale_concentrated_market")
            api_results["top_trusted_whale_present"] = top_trusted is not None
            api_results["top_trusted_whale_keys"] = sorted(top_trusted.keys()) if isinstance(top_trusted, dict) else []
            api_results["most_whale_market_present"] = most_whale_market is not None
            api_results["most_whale_market_keys"] = (
                sorted(most_whale_market.keys()) if isinstance(most_whale_market, dict) else []
            )

        leaderboard_response = client.get("/api/leaderboards/trusted/latest")
        api_results["leaderboard_status"] = leaderboard_response.status_code
        if leaderboard_response.status_code == 200:
            leaderboard_payload = leaderboard_response.json()
            leaderboard = (leaderboard_payload or {}).get("leaderboard") or {}
            rows = leaderboard.get("rows") or []
            api_results["leaderboard_rows"] = len(rows)
            if rows:
                api_results["leaderboard_row_keys"] = sorted(rows[0].keys())

    expected_summary_keys = sorted(
        [
            "latest_ingestion",
            "most_whale_concentrated_market",
            "platform_coverage",
            "profitability_users",
            "resolved_markets_available",
            "resolved_markets_observed",
            "scoring_version",
            "top_trusted_whale",
            "trusted_whales",
            "whales_detected",
        ]
    )
    expected_trusted_keys = sorted(
        [
            "external_user_ref",
            "profitability_score",
            "sample_trade_count",
            "trust_score",
            "user_id",
        ]
    )
    expected_market_keys = sorted(
        ["market_slug", "price", "question", "trusted_whale_count", "whale_count"]
    )
    expected_leaderboard_row_keys = sorted(
        ["board_type", "external_user_ref", "leaderboard_id", "rank", "score_metric", "score_value", "user_id"]
    )

    results = [
        CheckResult(
            "week6_source_users",
            (not require_data) or source_user_count > 0,
            {"require_data": require_data, "source_user_count": source_user_count},
        ),
        CheckResult(
            "week6_latest_whale_scores",
            (not require_data) or latest_score_user_count >= source_user_count,
            {
                "require_data": require_data,
                "source_user_count": source_user_count,
                "latest_score_user_count": latest_score_user_count,
            },
        ),
        CheckResult(
            "week6_positive_whale_outputs",
            (not require_data) or (whale_count > 0 and trusted_whale_count > 0),
            {
                "require_data": require_data,
                "scoring_version": latest_scoring_version,
                "whale_count": whale_count,
                "trusted_whale_count": trusted_whale_count,
            },
        ),
        CheckResult(
            "week6_dashboard_public_raw",
            (not require_data) or latest_public_raw_count > 0,
            {
                "require_data": require_data,
                "latest_dashboard_id": latest_dashboard_id,
                "public_raw_rows": latest_public_raw_count,
                "internal_trusted_rows": latest_trusted_count,
            },
        ),
        CheckResult(
            "week6_dashboard_market_fields",
            (not require_data) or latest_market_fields_count == latest_market_row_count,
            {
                "require_data": require_data,
                "latest_dashboard_id": latest_dashboard_id,
                "market_rows": latest_market_row_count,
                "rows_with_whale_fields": latest_market_fields_count,
            },
        ),
        CheckResult(
            "week6_api_home_summary_contract",
            api_results["home_summary_status"] == 200 and api_results["home_summary_keys"] == expected_summary_keys,
            {
                "status_code": api_results["home_summary_status"],
                "expected_keys": expected_summary_keys,
                "found_keys": api_results["home_summary_keys"],
            },
        ),
        CheckResult(
            "week6_api_home_summary_counts",
            api_results["home_summary_status"] == 200
            and api_results["summary_whales_detected"] == whale_count
            and api_results["summary_trusted_whales"] == trusted_whale_count
            and api_results["summary_scoring_version"] == latest_scoring_version,
            {
                "status_code": api_results["home_summary_status"],
                "summary_whales_detected": api_results["summary_whales_detected"],
                "db_whale_count": whale_count,
                "summary_trusted_whales": api_results["summary_trusted_whales"],
                "db_trusted_whales": trusted_whale_count,
                "summary_scoring_version": api_results["summary_scoring_version"],
                "db_scoring_version": latest_scoring_version,
            },
        ),
        CheckResult(
            "week6_api_home_summary_objects",
            api_results["home_summary_status"] == 200
            and ((trusted_whale_count == 0) or (
                api_results["top_trusted_whale_present"]
                and api_results["top_trusted_whale_keys"] == expected_trusted_keys
            ))
            and ((latest_market_row_count == 0) or (
                api_results["most_whale_market_present"]
                and api_results["most_whale_market_keys"] == expected_market_keys
            )),
            {
                "status_code": api_results["home_summary_status"],
                "trusted_whale_count": trusted_whale_count,
                "top_trusted_whale_present": api_results["top_trusted_whale_present"],
                "top_trusted_whale_keys": api_results["top_trusted_whale_keys"],
                "expected_top_trusted_whale_keys": expected_trusted_keys,
                "market_row_count": latest_market_row_count,
                "most_whale_market_present": api_results["most_whale_market_present"],
                "most_whale_market_keys": api_results["most_whale_market_keys"],
                "expected_market_keys": expected_market_keys,
            },
        ),
        CheckResult(
            "week6_api_trusted_leaderboard",
            api_results["leaderboard_status"] == 200
            and ((not require_data) or api_results["leaderboard_rows"] > 0)
            and ((api_results["leaderboard_rows"] == 0) or api_results["leaderboard_row_keys"] == expected_leaderboard_row_keys),
            {
                "require_data": require_data,
                "status_code": api_results["leaderboard_status"],
                "leaderboard_rows": api_results["leaderboard_rows"],
                "leaderboard_row_keys": api_results["leaderboard_row_keys"],
                "expected_row_keys": expected_leaderboard_row_keys,
            },
        ),
    ]
    return results


def _render_text(results: list[CheckResult]) -> str:
    """Render human-readable output."""
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
    parser = argparse.ArgumentParser(description="Validate the preliminary whale scoring outputs.")
    parser.add_argument(
        "--build",
        action="store_true",
        help="Build whale scores and a dashboard snapshot before validating.",
    )
    parser.add_argument(
        "--require-data",
        action="store_true",
        help="Fail when scored users or dashboard rows are missing.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    if args.build:
        with session_scope() as session:
            build_whale_score_snapshot(session)
            build_dashboard_snapshot(session)
    results = _run_checks(require_data=args.require_data)
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
