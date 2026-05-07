"""Validate Finance focus-domain matching."""

from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.services.market_scope import canonicalize_focus_domains, matched_focus_domains


def main() -> int:
    """CLI entrypoint."""
    checks = [
        {
            "name": "finance_aliases",
            "ok": canonicalize_focus_domains(["finance", "stocks", "macro"]) == ["finance"],
        },
        {
            "name": "fed_rates_match_finance",
            "ok": "finance"
            in matched_focus_domains(["Will the Fed cut interest rates before July?"], ["finance"]),
        },
        {
            "name": "earnings_match_finance",
            "ok": "finance"
            in matched_focus_domains(["Will Tesla earnings beat revenue guidance?"], ["finance"]),
        },
        {
            "name": "crypto_remains_separate",
            "ok": "finance"
            not in matched_focus_domains(["Will Bitcoin ETF volume pass $1b?"], ["finance"]),
        },
        {
            "name": "technology_ai_not_finance",
            "ok": "finance"
            not in matched_focus_domains(["Will OpenAI release a new model this month?"], ["finance"]),
        },
        {
            "name": "esports_still_video_games",
            "ok": "video-games"
            in matched_focus_domains(["Will Team Liquid win the Valorant event?"], ["video-games"]),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
