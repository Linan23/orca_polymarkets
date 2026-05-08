"""Warm cache-backed dashboard reads after API restart or analytics refresh."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from time import monotonic
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


DEFAULT_TIMEFRAMES = ("all", "90d", "30d", "7d")


@dataclass
class WarmResult:
    path: str
    ok: bool
    status: int | None
    duration_ms: int
    detail: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warm public Orca dashboard API caches.")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("ORCA_API_BASE_URL", "http://127.0.0.1:8001"),
        help="Base URL for the running FastAPI service.",
    )
    parser.add_argument("--market-limit", type=int, default=20)
    parser.add_argument("--preview-limit", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    return parser.parse_args()


def request_json(base_url: str, path: str, timeout_seconds: float) -> tuple[int, dict[str, Any] | None]:
    url = f"{base_url.rstrip('/')}{path}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "orca-cache-warmer/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read()
        if not body:
            return response.status, None
        return response.status, json.loads(body.decode("utf-8"))


def warm_path(base_url: str, path: str, timeout_seconds: float) -> tuple[WarmResult, dict[str, Any] | None]:
    started = monotonic()
    try:
        status, payload = request_json(base_url, path, timeout_seconds)
        return (
            WarmResult(path=path, ok=200 <= status < 300, status=status, duration_ms=int((monotonic() - started) * 1000)),
            payload,
        )
    except HTTPError as exc:
        return (
            WarmResult(
                path=path,
                ok=False,
                status=exc.code,
                duration_ms=int((monotonic() - started) * 1000),
                detail=str(exc.reason),
            ),
            None,
        )
    except (OSError, URLError, json.JSONDecodeError) as exc:
        return (
            WarmResult(
                path=path,
                ok=False,
                status=None,
                duration_ms=int((monotonic() - started) * 1000),
                detail=str(exc),
            ),
            None,
        )


def result_record(result: WarmResult) -> dict[str, Any]:
    return {
        "path": result.path,
        "ok": result.ok,
        "status": result.status,
        "duration_ms": result.duration_ms,
        "detail": result.detail,
    }


def main() -> int:
    args = parse_args()
    results: list[WarmResult] = []

    base_paths = ["/health"]
    for timeframe in DEFAULT_TIMEFRAMES:
        params = urlencode({"timeframe": timeframe, "limit": int(args.preview_limit)})
        base_paths.append(f"/api/dashboard/home?{params}")
    base_paths.extend(
        [
            f"/api/dashboards/latest/markets?{urlencode({'limit': int(args.market_limit)})}",
            f"/api/whales/latest?{urlencode({'limit': 250, 'tier': 'all'})}",
        ]
    )

    markets_payload: dict[str, Any] | None = None
    for path in base_paths:
        result, payload = warm_path(args.api_base_url, path, args.timeout_seconds)
        results.append(result)
        if path.startswith("/api/dashboards/latest/markets") and payload is not None:
            markets_payload = payload

    markets = markets_payload.get("markets") if markets_payload else None
    items = markets.get("items", []) if isinstance(markets, dict) else []
    slugs = [
        str(item.get("market_slug", "")).strip()
        for item in items
        if isinstance(item, dict) and str(item.get("market_slug", "")).strip()
    ][: int(args.market_limit)]

    for slug in slugs:
        params = urlencode({"top_whales_limit": int(args.preview_limit)})
        result, _payload = warm_path(
            args.api_base_url,
            f"/api/markets/{quote(slug, safe='')}/profile/full?{params}",
            args.timeout_seconds,
        )
        results.append(result)

    ok = all(result.ok for result in results)
    print(
        json.dumps(
            {
                "ok": ok,
                "api_base_url": args.api_base_url,
                "warmed_paths": len(results),
                "market_profiles": len(slugs),
                "results": [result_record(result) for result in results],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
