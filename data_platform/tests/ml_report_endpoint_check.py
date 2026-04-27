"""Validate the Week 10-11 ML report metadata endpoint."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.api.server import app


def main() -> int:
    """CLI entrypoint."""
    with TestClient(app) as client:
        response = client.get("/api/ml/week10-11/residual-movement")

    payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
    report: dict[str, Any] = payload.get("report", {}) if isinstance(payload, dict) else {}
    checks = [
        {
            "name": "endpoint_ok",
            "ok": response.status_code == 200,
            "status_code": response.status_code,
        },
        {
            "name": "polymarket_scope",
            "ok": report.get("scope", {}).get("market") == "polymarket",
            "scope": report.get("scope"),
        },
        {
            "name": "ridge_selected",
            "ok": report.get("selected_model", {}).get("estimator") == "ridge",
            "selected_model": report.get("selected_model"),
        },
        {
            "name": "client_update_present",
            "ok": "Current Client-Facing Claim" in str(report.get("client_update_markdown") or ""),
        },
        {
            "name": "tracked_report_present",
            "ok": "Week 10-11 Residual Whale Movement ML Report"
            in str(report.get("tracked_report_markdown") or ""),
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
