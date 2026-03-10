"""Data quality checks for normalized analytics tables."""

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


@dataclass
class QualityCheck:
    """One data-quality check result."""

    name: str
    ok: bool
    details: dict[str, Any]


def _count(session: Any, table_name: str) -> int:
    schema_name, short_name = table_name.split(".", 1)
    return int(session.execute(text(f'SELECT COUNT(*) FROM "{schema_name}"."{short_name}"')).scalar_one())


def run_checks(require_data: bool) -> list[QualityCheck]:
    """Run data quality checks and return structured results."""
    results: list[QualityCheck] = []

    with session_scope() as session:
        core_counts = {
            "analytics.market_event": _count(session, "analytics.market_event"),
            "analytics.market_contract": _count(session, "analytics.market_contract"),
            "analytics.transaction_fact": _count(session, "analytics.transaction_fact"),
            "analytics.position_snapshot": _count(session, "analytics.position_snapshot"),
            "analytics.orderbook_snapshot": _count(session, "analytics.orderbook_snapshot"),
        }
        has_any_data = any(value > 0 for value in core_counts.values())
        results.append(QualityCheck("core_table_counts", (not require_data) or has_any_data, {"counts": core_counts}))

        # Duplicate source transaction IDs per platform should never exist.
        duplicate_tx_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*) FROM (
                      SELECT platform_id, source_transaction_id, count(*) AS c
                      FROM analytics.transaction_fact
                      GROUP BY platform_id, source_transaction_id
                      HAVING count(*) > 1
                    ) d
                    """
                )
            ).scalar_one()
        )
        results.append(QualityCheck("duplicate_source_transactions", duplicate_tx_count == 0, {"duplicates": duplicate_tx_count}))

        # Transaction side labels should be normalized lowercase.
        non_normalized_side_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM analytics.transaction_fact
                    WHERE side IS NOT NULL
                      AND side <> lower(side)
                    """
                )
            ).scalar_one()
        )
        results.append(
            QualityCheck(
                "transaction_side_lowercase",
                non_normalized_side_count == 0,
                {"non_normalized_rows": non_normalized_side_count},
            )
        )

        # Wallet addresses should be canonical lowercase where present.
        non_normalized_wallet_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM analytics.user_account
                    WHERE wallet_address IS NOT NULL
                      AND wallet_address ~ '[A-F]'
                    """
                )
            ).scalar_one()
        )
        results.append(
            QualityCheck(
                "wallet_address_lowercase",
                non_normalized_wallet_count == 0,
                {"non_normalized_rows": non_normalized_wallet_count},
            )
        )

        # Invalid spread: negative value where both bid/ask exist.
        negative_spread_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM analytics.orderbook_snapshot
                    WHERE best_bid IS NOT NULL
                      AND best_ask IS NOT NULL
                      AND spread IS NOT NULL
                      AND spread < 0
                    """
                )
            ).scalar_one()
        )
        results.append(QualityCheck("orderbook_negative_spread", negative_spread_count == 0, {"negative_spreads": negative_spread_count}))

        # Future-dated transaction times beyond a small clock-skew tolerance.
        future_tx_count = int(
            session.execute(
                text(
                    """
                    SELECT count(*)
                    FROM analytics.transaction_fact
                    WHERE transaction_time > (now() + interval '5 minutes')
                    """
                )
            ).scalar_one()
        )
        results.append(QualityCheck("future_transaction_timestamps", future_tx_count == 0, {"future_rows": future_tx_count}))

        # Freshness signal: latest scrape run should exist when require_data is set.
        latest_scrape_present = bool(
            session.execute(text("SELECT 1 FROM analytics.scrape_run ORDER BY scrape_run_id DESC LIMIT 1")).first()
        )
        results.append(
            QualityCheck(
                "latest_scrape_present",
                (not require_data) or latest_scrape_present,
                {"present": latest_scrape_present},
            )
        )

    return results


def render_text(results: list[QualityCheck]) -> str:
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
    parser = argparse.ArgumentParser(description="Run data-quality checks against the analytics database.")
    parser.add_argument("--require-data", action="store_true", help="Fail if core tables are all empty.")
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results = run_checks(require_data=args.require_data)
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
