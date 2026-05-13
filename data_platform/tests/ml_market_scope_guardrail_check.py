"""Validate ML market-scope guardrails for physical sports vs esports."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.services.market_scope import is_physical_sports_market


REPORT_PATHS = (
    Path("data_platform/ml/ML_TREND_DIRECTION_CLASSIFIER_CURRENT_DB_ASOF.json"),
    Path("data_platform/ml/ML_TREND_SIMILARITY_CURRENT_DB_ASOF.json"),
    Path("data_platform/ml/ML_WHALE_ANCHORED_DELTA_CURRENT_DB_ASOF.json"),
    Path("data_platform/ml/EXAMPLE_MARKET_PROJECTION_RIDGE_TRADE_COVERED.json"),
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    """CLI entrypoint."""
    reports = {str(path): _read_json(path) for path in REPORT_PATHS}
    anchored = reports[str(Path("data_platform/ml/ML_WHALE_ANCHORED_DELTA_CURRENT_DB_ASOF.json"))]
    event_segments = [
        segment
        for window in anchored.get("windows", {}).values()
        for segment in window.get("event_category_segments", [])
    ]
    checks = [
        {
            "name": "physical_sports_detected",
            "ok": is_physical_sports_market(["NBA: Lakers vs Celtics", "nba-lal-bos"], category="sports"),
        },
        {
            "name": "esports_retained",
            "ok": not is_physical_sports_market(
                ["Counter-Strike: fnatic vs Leo Team", "cs2-fnc-leo2-2026-04-27"],
                category="esports",
            ),
        },
        {
            "name": "video_games_retained",
            "ok": not is_physical_sports_market(["Will GTA VI release before June?"], category="sports"),
        },
        {
            "name": "miscategorized_geopolitics_retained",
            "ok": not is_physical_sports_market(
                ["Iran x Israel/US conflict ends by April 7?"],
                category="sports",
            ),
        },
        {
            "name": "scope_metadata_present",
            "ok": all(
                "esports" in str(report.get("market_scope_note") or "").casefold()
                and "physical sports" in str(report.get("market_scope_note") or "").casefold()
                for report in reports.values()
                if report
            ),
            "paths": [path for path, report in reports.items() if report],
        },
        {
            "name": "anchored_event_category_segments_present",
            "ok": any(str(segment.get("category")) == "esports" for segment in event_segments),
            "event_categories": sorted({str(segment.get("category")) for segment in event_segments}),
        },
    ]
    ok = all(bool(check["ok"]) for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
