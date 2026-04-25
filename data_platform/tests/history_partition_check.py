"""Validation checks for history tables, partition shadows, and compatibility views."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.db.session import session_scope
from data_platform.settings import get_settings


REQUIRED_OBJECTS = {
    "analytics.user_account_history": "table",
    "analytics.market_event_history": "table",
    "analytics.market_contract_history": "table",
    "analytics.market_tag_map_history": "table",
    "analytics.orderbook_snapshot_hourly": "table",
    "analytics.orderbook_snapshot_daily": "table",
    "analytics.position_snapshot_daily": "table",
    "analytics.scrape_run_part": "table",
    "raw.api_payload_part": "table",
    "analytics.transaction_fact_part": "table",
    "analytics.orderbook_snapshot_part": "table",
    "analytics.position_snapshot_part": "table",
    "analytics.whale_score_snapshot_part": "table",
    "analytics.scrape_run_all": "view",
    "raw.api_payload_all": "view",
    "analytics.transaction_fact_all": "view",
    "analytics.orderbook_snapshot_all": "view",
    "analytics.position_snapshot_all": "view",
    "analytics.whale_score_snapshot_all": "view",
}


@dataclass
class CheckResult:
    name: str
    ok: bool
    details: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate history/partition lifecycle rollout state.")
    parser.add_argument("--database-url", default=get_settings().database_url)
    parser.add_argument(
        "--allow-empty-position-snapshots",
        action="store_true",
        help="Allow zero rows in position snapshot legacy/shadow tables for scoped crawls without tracked positions.",
    )
    return parser.parse_args()


def _count(session: Any, qualified_name: str) -> int:
    schema_name, table_name = qualified_name.split(".", 1)
    return int(session.execute(text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')).scalar_one())


def run_checks(database_url: str, *, allow_empty_position_snapshots: bool = False) -> list[CheckResult]:
    results: list[CheckResult] = []
    with session_scope(database_url) as session:
        rows = session.execute(
            text(
                """
                SELECT table_schema || '.' || table_name AS qualified_name, 'table' AS object_type
                FROM information_schema.tables
                WHERE table_schema IN ('analytics', 'raw')
                UNION ALL
                SELECT table_schema || '.' || table_name AS qualified_name, 'view' AS object_type
                FROM information_schema.views
                WHERE table_schema IN ('analytics', 'raw')
                """
            )
        ).all()
        object_map = {str(row.qualified_name): str(row.object_type) for row in rows}
        missing = [name for name, object_type in REQUIRED_OBJECTS.items() if object_map.get(name) != object_type]
        results.append(CheckResult("required_objects", not missing, {"missing": missing}))

        comparisons = [
            ("analytics.user_account", "analytics.user_account_history", "user_id"),
            ("analytics.market_event", "analytics.market_event_history", "event_id"),
            ("analytics.market_contract", "analytics.market_contract_history", "market_contract_id"),
        ]
        for current_table, history_table, key_name in comparisons:
            current_count = _count(session, current_table)
            current_history_count = int(
                session.execute(
                    text(
                        f'SELECT COUNT(*) FROM "{history_table.split(".",1)[0]}"."{history_table.split(".",1)[1]}" WHERE is_current'
                    )
                ).scalar_one()
            )
            results.append(
                CheckResult(
                    f"history_current_alignment:{history_table}",
                    current_count == current_history_count,
                    {"current_count": current_count, "history_current_count": current_history_count, "key": key_name},
                )
            )

        tag_current = _count(session, "analytics.market_tag_map")
        tag_history_current = int(session.execute(text("SELECT COUNT(*) FROM analytics.market_tag_map_history WHERE is_current")).scalar_one())
        results.append(
            CheckResult(
                "history_current_alignment:analytics.market_tag_map_history",
                tag_current == tag_history_current,
                {"current_count": tag_current, "history_current_count": tag_history_current},
            )
        )

        for legacy_table, compatibility_view in (
            ("analytics.scrape_run", "analytics.scrape_run_all"),
            ("raw.api_payload", "raw.api_payload_all"),
            ("analytics.transaction_fact", "analytics.transaction_fact_all"),
            ("analytics.orderbook_snapshot", "analytics.orderbook_snapshot_all"),
            ("analytics.position_snapshot", "analytics.position_snapshot_all"),
            ("analytics.whale_score_snapshot", "analytics.whale_score_snapshot_all"),
        ):
            legacy_count = _count(session, legacy_table)
            view_count = _count(session, compatibility_view)
            results.append(
                CheckResult(
                    f"compatibility_view_count:{compatibility_view}",
                    view_count == legacy_count,
                    {"legacy_count": legacy_count, "view_count": view_count},
                )
            )

        shadow_counts = {
            "analytics.scrape_run_part": _count(session, "analytics.scrape_run_part"),
            "raw.api_payload_part": _count(session, "raw.api_payload_part"),
            "analytics.transaction_fact_part": _count(session, "analytics.transaction_fact_part"),
            "analytics.orderbook_snapshot_part": _count(session, "analytics.orderbook_snapshot_part"),
            "analytics.position_snapshot_part": _count(session, "analytics.position_snapshot_part"),
            "analytics.whale_score_snapshot_part": _count(session, "analytics.whale_score_snapshot_part"),
        }
        required_shadow_tables = {name for name in shadow_counts if name != "analytics.position_snapshot_part"}
        if not allow_empty_position_snapshots:
            required_shadow_tables.add("analytics.position_snapshot_part")
        results.append(
            CheckResult(
                "shadow_tables_populated",
                all(shadow_counts[name] > 0 for name in required_shadow_tables),
                {**shadow_counts, "allow_empty_position_snapshots": allow_empty_position_snapshots},
            )
        )

        partition_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_inherits
                    JOIN pg_class c ON c.oid = inhrelid
                    JOIN pg_class p ON p.oid = inhparent
                    JOIN pg_namespace n ON n.oid = p.relnamespace
                    WHERE (n.nspname = 'analytics' AND p.relname IN ('scrape_run_part', 'transaction_fact_part', 'orderbook_snapshot_part', 'position_snapshot_part', 'whale_score_snapshot_part'))
                       OR (n.nspname = 'raw' AND p.relname = 'api_payload_part')
                    """
                )
            ).scalar_one()
        )
        results.append(CheckResult("partition_children_present", partition_count >= 6, {"partition_count": partition_count}))
    return results


def main() -> int:
    args = parse_args()
    results = run_checks(
        args.database_url,
        allow_empty_position_snapshots=args.allow_empty_position_snapshots,
    )
    ok = all(item.ok for item in results)
    payload = {"ok": ok, "checks": [asdict(item) for item in results]}
    print(json.dumps(payload, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
