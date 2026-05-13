"""Validate generated ML category validation artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BACKFILLED_REPORT_PATH = Path("data_platform/ml/CATEGORY_VALIDATION_BACKFILLED_SECOND_RIDGE_TRADE_COVERED.json")
CURRENT_DB_REPORT_PATH = Path(
    "data_platform/ml/CATEGORY_VALIDATION_CURRENT_DB_ASOF_SNAPSHOT_RIDGE_TRADE_COVERED.json"
)


def _read_report(path: Path) -> dict[str, Any]:
    """Read a generated category validation report."""
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    """CLI entrypoint."""
    backfilled = _read_report(BACKFILLED_REPORT_PATH)
    current_db = _read_report(CURRENT_DB_REPORT_PATH)
    categories = backfilled.get("categories", [])
    checks = [
        {
            "name": "backfilled_report_present",
            "ok": bool(backfilled),
            "path": str(BACKFILLED_REPORT_PATH),
        },
        {
            "name": "current_db_report_present",
            "ok": bool(current_db),
            "path": str(CURRENT_DB_REPORT_PATH),
        },
        {
            "name": "multiple_categories_tested",
            "ok": int(backfilled.get("overall", {}).get("category_count") or 0) >= 5,
            "category_count": backfilled.get("overall", {}).get("category_count"),
        },
        {
            "name": "multiple_prediction_cases_present",
            "ok": sum(1 for category in categories if len(category.get("cases", [])) >= 3) >= 5,
            "categories_with_three_or_more_cases": sum(
                1 for category in categories if len(category.get("cases", [])) >= 3
            ),
        },
        {
            "name": "window_metrics_present",
            "ok": all(
                int(backfilled.get("overall", {}).get("windows", {}).get(window_name, {}).get("row_count") or 0) > 0
                for window_name in ("12h", "24h")
            ),
            "windows": backfilled.get("overall", {}).get("windows", {}),
        },
        {
            "name": "diagnostics_present",
            "ok": bool(backfilled.get("overall", {}).get("top_issue_counts")),
            "top_issue_counts": backfilled.get("overall", {}).get("top_issue_counts", [])[:5],
        },
        {
            "name": "current_db_expanded_coverage_present",
            "ok": int(current_db.get("overall", {}).get("row_count") or 0) >= 1000
            and int(current_db.get("overall", {}).get("category_count") or 0) >= 5
            and not current_db.get("overall", {}).get("prediction_error"),
            "row_count": current_db.get("overall", {}).get("row_count"),
            "category_count": current_db.get("overall", {}).get("category_count"),
            "prediction_error": current_db.get("overall", {}).get("prediction_error"),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
