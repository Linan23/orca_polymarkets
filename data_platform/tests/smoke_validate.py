"""Lightweight smoke validation for the local data platform stack."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient
from sqlalchemy import text

from data_platform.api.server import app
from data_platform.services.account_auth import SESSION_COOKIE_NAME
from data_platform.db.bootstrap import create_database_objects
from data_platform.db.session import session_scope
from data_platform.services.dashboard_builder import build_dashboard_snapshot
from data_platform.services.whale_scoring import build_whale_score_snapshot
from data_platform.tests.history_partition_check import run_checks as run_history_partition_checks


REQUIRED_TABLES = {
    "app.app_account",
    "app.app_account_preferences",
    "app.app_session",
    "app.app_watchlist_market",
    "app.app_watchlist_user",
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
REQUIRED_INDEXES = {
    "analytics.ix_dashboard_market_dashboard_slug",
    "analytics.ix_market_contract_market_slug",
    "analytics.ix_market_profile_dashboard_market",
    "analytics.ix_position_snapshot_user_market_time",
    "analytics.ix_transaction_fact_user_market_time",
    "analytics.ix_transaction_fact_user_time",
    "analytics.ix_user_profile_dashboard_user",
    "analytics.ix_whale_score_snapshot_batch_user",
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
    "/api/home/summary",
    "/api/analytics/top-profitable-users?limit=1",
    "/api/analytics/market-whale-concentration?limit=1",
    "/api/analytics/whale-entry-behavior?limit=1",
    "/api/analytics/recent-whale-entries?limit=1",
    "/api/analytics/top-profitable-users?limit=1&timeframe=30d",
    "/api/markets?limit=1",
    "/api/users?limit=1",
    "/api/transactions?limit=1",
    "/api/positions?limit=1",
    "/api/whales/latest?limit=1",
    "/api/leaderboards/trusted/latest",
    "/api/leaderboards/latest",
    "/api/dashboards/latest",
    "/api/dashboards/latest/markets?limit=1",
)


@dataclass
class CheckResult:
    """Represent one smoke-check result."""

    name: str
    ok: bool
    details: dict[str, Any]


def _expected_alembic_revisions() -> tuple[str, ...]:
    """Return the Alembic head revisions declared by this checkout."""
    config = Config(str(REPO_ROOT / "alembic.ini"))
    script_directory = ScriptDirectory.from_config(config)
    return tuple(sorted(script_directory.get_heads()))


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

        expected_revisions = _expected_alembic_revisions()
        revisions = tuple(
            session.execute(text("SELECT version_num FROM alembic_version ORDER BY version_num")).scalars().all()
        )
        results.append(
            CheckResult(
                "alembic_revision",
                revisions == expected_revisions,
                {"expected": list(expected_revisions), "found": list(revisions)},
            )
        )

        rows = session.execute(
            text(
                """
                SELECT table_schema || '.' || table_name AS table_name
                FROM information_schema.tables
                WHERE table_schema IN ('app', 'analytics', 'raw')
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

        index_rows = session.execute(
            text(
                """
                SELECT schemaname || '.' || indexname AS index_name
                FROM pg_indexes
                WHERE schemaname IN ('analytics', 'app', 'raw')
                """
            )
        ).scalars()
        found_indexes = set(index_rows)
        missing_indexes = sorted(REQUIRED_INDEXES - found_indexes)
        results.append(
            CheckResult(
                "required_indexes",
                not missing_indexes,
                {"required": len(REQUIRED_INDEXES), "found": len(found_indexes), "missing": missing_indexes},
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

        preferred_username_count = int(
            session.execute(
                text("SELECT COUNT(*) FROM analytics.user_account WHERE COALESCE(preferred_username, '') <> ''")
            ).scalar_one()
        )
        named_polymarket_wallets = int(
            session.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT lower(COALESCE(trade->>'proxyWallet', '')))
                    FROM raw.api_payload payload_row
                    CROSS JOIN LATERAL jsonb_array_elements(payload_row.payload->'trades') AS trade
                    WHERE payload_row.entity_type = 'trades'
                      AND COALESCE(trade->>'name', '') <> ''
                      AND NOT (trade->>'name' ~* '^0x[0-9a-f]{8,}(-[0-9]+)?$')
                    """
                )
            ).scalar_one()
        )
        results.append(
            CheckResult(
                "preferred_username_backfill",
                (not require_sample_data) or named_polymarket_wallets == 0 or preferred_username_count > 0,
                {
                    "required": require_sample_data,
                    "named_polymarket_wallets": named_polymarket_wallets,
                    "preferred_username_count": preferred_username_count,
                },
            )
        )
    return results


def _run_api_checks() -> list[CheckResult]:
    """Run in-process FastAPI endpoint smoke checks."""
    results: list[CheckResult] = []
    historical_market_slug: str | None = None
    with session_scope() as session:
        historical_market_slug = session.execute(
            text(
                """
                WITH latest_dashboard AS (
                  SELECT dashboard_id
                  FROM analytics.dashboard
                  ORDER BY generated_at DESC, dashboard_id DESC
                  LIMIT 1
                )
                SELECT mc.market_slug
                FROM analytics.market_contract mc
                LEFT JOIN analytics.dashboard_market dm
                  ON dm.dashboard_id = (SELECT dashboard_id FROM latest_dashboard)
                 AND dm.market_slug = mc.market_slug
                WHERE mc.market_slug IS NOT NULL
                  AND dm.market_slug IS NULL
                ORDER BY mc.updated_at DESC NULLS LAST, mc.market_contract_id DESC
                LIMIT 1
                """
            )
        ).scalar_one_or_none()

    with TestClient(app) as client:
        candidate_user_id: int | None = None
        candidate_market_slug: str | None = None
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

        whale_response = client.get("/api/whales/latest?limit=1")
        top_profitable_response = client.get("/api/analytics/top-profitable-users?limit=1")
        if top_profitable_response.status_code == 200:
            top_profitable_payload = top_profitable_response.json()
            top_profitable_items = (((top_profitable_payload or {}).get("analytics") or {}).get("items") or [])
            if top_profitable_items:
                results.append(
                    CheckResult(
                        "api:/api/analytics/top-profitable-users fields",
                        {"sample_trade_count", "latest_trade_time"}.issubset(top_profitable_items[0].keys()),
                        {"payload_keys": sorted(top_profitable_items[0].keys())},
                    )
                )

        market_concentration_response = client.get("/api/analytics/market-whale-concentration?limit=1")
        if market_concentration_response.status_code == 200:
            market_concentration_payload = market_concentration_response.json()
            market_concentration_items = (((market_concentration_payload or {}).get("analytics") or {}).get("items") or [])
            if market_concentration_items:
                results.append(
                    CheckResult(
                        "api:/api/analytics/market-whale-concentration fields",
                        {"last_entry_time", "market_status_label", "whale_bias_label"}.issubset(
                            market_concentration_items[0].keys()
                        ),
                        {"payload_keys": sorted(market_concentration_items[0].keys())},
                    )
                )

        whale_entry_response = client.get("/api/analytics/whale-entry-behavior?limit=1")
        if whale_entry_response.status_code == 200:
            whale_entry_payload = whale_entry_response.json()
            whale_entry_items = (((whale_entry_payload or {}).get("analytics") or {}).get("items") or [])
            if whale_entry_items:
                results.append(
                    CheckResult(
                        "api:/api/analytics/whale-entry-behavior fields",
                        {
                            "weighted_current_price",
                            "yes_entry_trade_count",
                            "no_entry_trade_count",
                            "last_entry_time",
                            "entry_edge",
                        }.issubset(whale_entry_items[0].keys()),
                        {"payload_keys": sorted(whale_entry_items[0].keys())},
                    )
                )

        recent_entry_response = client.get("/api/analytics/recent-whale-entries?limit=1")
        if recent_entry_response.status_code == 200:
            recent_entry_payload = recent_entry_response.json()
            recent_entry_items = (((recent_entry_payload or {}).get("analytics") or {}).get("items") or [])
            if recent_entry_items:
                results.append(
                    CheckResult(
                        "api:/api/analytics/recent-whale-entries fields",
                        {
                            "entry_trade_count",
                            "total_entry_notional",
                            "latest_entry_time",
                            "market_status_label",
                            "whale_bias_label",
                        }.issubset(recent_entry_items[0].keys()),
                        {"payload_keys": sorted(recent_entry_items[0].keys())},
                    )
                )

        if whale_response.status_code == 200:
            whale_payload = whale_response.json()
            whale_items = (((whale_payload or {}).get("whales") or {}).get("items") or [])
            potential_tier_response = client.get("/api/whales/latest?limit=3&tier=potential")
            potential_tier_ok = potential_tier_response.status_code == 200
            try:
                potential_tier_payload = potential_tier_response.json()
            except ValueError:
                potential_tier_payload = {"raw_body": potential_tier_response.text[:200]}
            potential_tier_items = (((potential_tier_payload or {}).get("whales") or {}).get("items") or [])
            potential_tier_flags_ok = all(
                not bool(item.get("is_whale")) and not bool(item.get("is_trusted_whale"))
                for item in potential_tier_items
            )
            results.append(
                CheckResult(
                    "api:/api/whales/latest?tier=potential",
                    potential_tier_ok and potential_tier_flags_ok,
                    {
                        "status_code": potential_tier_response.status_code,
                        "item_count": len(potential_tier_items),
                    },
                )
            )
            standard_tier_response = client.get("/api/whales/latest?limit=3&tier=standard")
            standard_tier_ok = standard_tier_response.status_code == 200
            try:
                standard_tier_payload = standard_tier_response.json()
            except ValueError:
                standard_tier_payload = {"raw_body": standard_tier_response.text[:200]}
            standard_tier_items = (((standard_tier_payload or {}).get("whales") or {}).get("items") or [])
            standard_tier_flags_ok = all(
                not bool(item.get("is_whale")) and not bool(item.get("is_trusted_whale"))
                for item in standard_tier_items
            )
            results.append(
                CheckResult(
                    "api:/api/whales/latest?tier=standard",
                    standard_tier_ok and standard_tier_flags_ok,
                    {
                        "status_code": standard_tier_response.status_code,
                        "item_count": len(standard_tier_items),
                    },
                )
            )
            if whale_items:
                results.append(
                    CheckResult(
                        "api:/api/whales/latest identity_fields",
                        {"wallet_address", "preferred_username", "display_label"}.issubset(whale_items[0].keys()),
                        {"payload_keys": sorted(whale_items[0].keys())},
                    )
                )
                user_id = whale_items[0].get("user_id")
                if user_id is not None:
                    candidate_user_id = int(user_id)
                    response = client.get(f"/api/users/{user_id}/whale-profile")
                    ok = response.status_code == 200
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {"raw_body": response.text[:200]}
                    results.append(
                        CheckResult(
                            "api:/api/users/{user_id}/whale-profile",
                            ok,
                            {
                                "user_id": user_id,
                                "status_code": response.status_code,
                                "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
                            },
                        )
                    )
                    if ok and isinstance(payload, dict):
                        profile = payload.get("profile") or {}
                        results.append(
                            CheckResult(
                                "api:/api/users/{user_id}/whale-profile identity_fields",
                                {"wallet_address", "preferred_username", "display_label"}.issubset(profile.keys()),
                                {"payload_keys": sorted(profile.keys()) if isinstance(profile, dict) else []},
                            )
                        )

                    insights_response = client.get(f"/api/users/{user_id}/activity-insights?timeframe=30d")
                    insights_ok = insights_response.status_code == 200
                    try:
                        insights_payload = insights_response.json()
                    except ValueError:
                        insights_payload = {"raw_body": insights_response.text[:200]}
                    details: dict[str, Any] = {
                        "user_id": user_id,
                        "status_code": insights_response.status_code,
                        "payload_keys": sorted(insights_payload.keys()) if isinstance(insights_payload, dict) else [],
                    }
                    if insights_ok and isinstance(insights_payload, dict):
                        insights = insights_payload.get("insights") or {}
                        details["hourly_bucket_count"] = len(insights.get("hourly_activity_utc") or [])
                        details["recent_trade_count"] = len(insights.get("recent_trades") or [])
                        details["position_count"] = len(insights.get("current_positions") or [])
                    results.append(
                        CheckResult(
                            "api:/api/users/{user_id}/activity-insights",
                            insights_ok,
                            details,
                        )
                    )

        market_response = client.get("/api/dashboards/latest/markets?limit=1")
        if market_response.status_code == 200:
            market_payload = market_response.json()
            market_items = (((market_payload or {}).get("markets") or {}).get("items") or [])
            if market_items:
                market_slug = market_items[0].get("market_slug")
                if market_slug:
                    candidate_market_slug = str(market_slug)
                    response = client.get(f"/api/markets/{market_slug}/profile")
                    ok = response.status_code == 200
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {"raw_body": response.text[:200]}
                    results.append(
                        CheckResult(
                            "api:/api/markets/{market_slug}/profile",
                            ok,
                            {
                                "market_slug": market_slug,
                                "status_code": response.status_code,
                                "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
                            },
                        )
                    )
        if historical_market_slug:
            response = client.get(f"/api/markets/{historical_market_slug}/profile")
            ok = response.status_code == 200
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_body": response.text[:200]}
            results.append(
                CheckResult(
                    "api:/api/markets/{market_slug}/profile historical_fallback",
                    ok,
                    {
                        "market_slug": historical_market_slug,
                        "status_code": response.status_code,
                        "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
                    },
                )
            )

        following_response = client.post(
            "/api/following/overview",
            json={
                "user_ids": [candidate_user_id] if candidate_user_id is not None else [],
                "market_slugs": [candidate_market_slug] if candidate_market_slug else [],
            },
        )
        following_ok = following_response.status_code == 200
        try:
            following_payload = following_response.json()
        except ValueError:
            following_payload = {"raw_body": following_response.text[:200]}
        results.append(
            CheckResult(
                "api:/api/following/overview",
                following_ok,
                {
                    "status_code": following_response.status_code,
                    "payload_keys": sorted(following_payload.keys()) if isinstance(following_payload, dict) else [],
                },
            )
        )
        if following_ok and isinstance(following_payload, dict):
            overview = following_payload.get("overview") or {}
            results.append(
                CheckResult(
                    "api:/api/following/overview keys",
                    {
                        "summary",
                        "inflow_24h",
                        "market_focus_recent",
                        "recent_closed_markets",
                        "trader_focus",
                    }.issubset(overview.keys()),
                    {"payload_keys": sorted(overview.keys()) if isinstance(overview, dict) else []},
                )
            )
            trader_focus_rows = overview.get("trader_focus") or []
            recent_closed_rows = overview.get("recent_closed_markets") or []
            status_rows_ok = True
            if trader_focus_rows:
                status_rows_ok = "market_status_label" in trader_focus_rows[0]
            if status_rows_ok and recent_closed_rows:
                status_rows_ok = {
                    "market_status_label",
                    "result_label",
                }.issubset(recent_closed_rows[0].keys())
            results.append(
                CheckResult(
                    "api:/api/following/overview market_labels",
                    status_rows_ok,
                    {
                        "trader_focus_count": len(trader_focus_rows),
                        "recent_closed_count": len(recent_closed_rows),
                    },
                )
            )

        following_dashboard_response = client.post(
            "/api/following/dashboard",
            json={
                "user_ids": [candidate_user_id] if candidate_user_id is not None else [],
                "market_slugs": [candidate_market_slug] if candidate_market_slug else [],
            },
        )
        following_dashboard_ok = following_dashboard_response.status_code == 200
        try:
            following_dashboard_payload = following_dashboard_response.json()
        except ValueError:
            following_dashboard_payload = {"raw_body": following_dashboard_response.text[:200]}
        results.append(
            CheckResult(
                "api:/api/following/dashboard",
                following_dashboard_ok,
                {
                    "status_code": following_dashboard_response.status_code,
                    "payload_keys": (
                        sorted(following_dashboard_payload.keys())
                        if isinstance(following_dashboard_payload, dict)
                        else []
                    ),
                },
            )
        )
        if following_dashboard_ok and isinstance(following_dashboard_payload, dict):
            dashboard_payload = following_dashboard_payload.get("dashboard") or {}
            results.append(
                CheckResult(
                    "api:/api/following/dashboard keys",
                    {
                        "overview",
                        "users",
                        "markets",
                    }.issubset(dashboard_payload.keys()),
                    {"payload_keys": sorted(dashboard_payload.keys()) if isinstance(dashboard_payload, dict) else []},
                )
            )

        auth_email = f"smoke-{uuid4().hex[:12]}@example.com"
        auth_password = "SmokePass123!"
        signup_response = client.post(
            "/api/auth/signup",
            json={
                "display_name": "Smoke User",
                "email": auth_email,
                "password": auth_password,
            },
        )
        signup_ok = signup_response.status_code == 200
        signup_payload: dict[str, Any]
        try:
            signup_payload = signup_response.json()
        except ValueError:
            signup_payload = {"raw_body": signup_response.text[:200]}
        results.append(
            CheckResult(
                "api:/api/auth/signup",
                signup_ok,
                {
                    "status_code": signup_response.status_code,
                    "has_cookie": SESSION_COOKIE_NAME in signup_response.cookies,
                    "payload_keys": sorted(signup_payload.keys()) if isinstance(signup_payload, dict) else [],
                },
            )
        )
        if signup_ok:
            with session_scope() as session:
                stored_password_hash = session.execute(
                    text("SELECT password_hash FROM app.app_account WHERE email = :email"),
                    {"email": auth_email},
                ).scalar_one_or_none()
            results.append(
                CheckResult(
                    "api:/api/auth/signup stored_hash",
                    bool(stored_password_hash) and stored_password_hash != auth_password,
                    {
                        "email": auth_email,
                        "password_hash_prefix": stored_password_hash[:32] if stored_password_hash else None,
                    },
                )
            )

        duplicate_signup_response = client.post(
            "/api/auth/signup",
            json={
                "display_name": "Smoke User",
                "email": auth_email,
                "password": auth_password,
            },
        )
        results.append(
            CheckResult(
                "api:/api/auth/signup duplicate_email",
                duplicate_signup_response.status_code == 409,
                {"status_code": duplicate_signup_response.status_code},
            )
        )

        auth_me_response = client.get("/api/auth/me")
        auth_me_ok = auth_me_response.status_code == 200
        try:
            auth_me_payload = auth_me_response.json()
        except ValueError:
            auth_me_payload = {"raw_body": auth_me_response.text[:200]}
        results.append(
            CheckResult(
                "api:/api/auth/me signed_in",
                auth_me_ok,
                {
                    "status_code": auth_me_response.status_code,
                    "payload_keys": sorted(auth_me_payload.keys()) if isinstance(auth_me_payload, dict) else [],
                },
            )
        )

        if candidate_user_id is not None:
            follow_user_response = client.post(f"/api/account/follow/users/{candidate_user_id}")
            follow_user_ok = follow_user_response.status_code == 200
            try:
                follow_user_payload = follow_user_response.json()
            except ValueError:
                follow_user_payload = {"raw_body": follow_user_response.text[:200]}
            results.append(
                CheckResult(
                    "api:/api/account/follow/users/{user_id}",
                    follow_user_ok,
                    {
                        "status_code": follow_user_response.status_code,
                        "watchlist": (follow_user_payload.get("watchlist") or {}) if isinstance(follow_user_payload, dict) else {},
                    },
                )
            )

        preferences_response = client.patch(
            "/api/account/preferences",
            json={
                "homepage": {"research_timeframe": "30d"},
                "leaderboard": {
                    "active_board": "user",
                    "user_filters": {"board": "potential", "min_trades": 5, "sort": "profitability"},
                    "market_filters": {"min_whales": 2, "sort": "whales"},
                },
            },
        )
        preferences_ok = preferences_response.status_code == 200
        try:
            preferences_payload = preferences_response.json()
        except ValueError:
            preferences_payload = {"raw_body": preferences_response.text[:200]}
        results.append(
            CheckResult(
                "api:/api/account/preferences valid_patch",
                preferences_ok,
                {
                    "status_code": preferences_response.status_code,
                    "payload_keys": sorted(preferences_payload.keys()) if isinstance(preferences_payload, dict) else [],
                },
            )
        )

        invalid_preferences_response = client.patch(
            "/api/account/preferences",
            json={"homepage": {"research_timeframe": "1d"}},
        )
        results.append(
            CheckResult(
                "api:/api/account/preferences invalid_patch",
                invalid_preferences_response.status_code == 422,
                {"status_code": invalid_preferences_response.status_code},
            )
        )

        with TestClient(app) as second_client:
            second_email = f"smoke-{uuid4().hex[:12]}@example.com"
            second_signup = second_client.post(
                "/api/auth/signup",
                json={
                    "display_name": "Second Smoke",
                    "email": second_email,
                    "password": auth_password,
                },
            )
            second_signup_ok = second_signup.status_code == 200
            results.append(
                CheckResult(
                    "api:/api/auth/signup second_account",
                    second_signup_ok,
                    {"status_code": second_signup.status_code},
                )
            )

            second_me_before = second_client.get("/api/auth/me")
            second_watchlist_before = (((second_me_before.json() if second_me_before.status_code == 200 else {}) or {}).get("session") or {}).get("watchlist") or {}
            results.append(
                CheckResult(
                    "api:/api/auth/me account_isolation_initial",
                    second_me_before.status_code == 200
                    and second_watchlist_before.get("users", []) == []
                    and second_watchlist_before.get("markets", []) == [],
                    {
                        "status_code": second_me_before.status_code,
                        "watchlist": second_watchlist_before,
                    },
                )
            )

            import_payload = {
                "user_ids": [candidate_user_id, candidate_user_id, 0] if candidate_user_id is not None else [],
                "market_slugs": [candidate_market_slug, candidate_market_slug, "missing-market"] if candidate_market_slug else [],
            }
            import_response = second_client.post("/api/account/watchlist/import-local", json=import_payload)
            import_ok = import_response.status_code == 200
            try:
                import_result = import_response.json()
            except ValueError:
                import_result = {"raw_body": import_response.text[:200]}
            imported_watchlist = (import_result.get("watchlist") or {}) if isinstance(import_result, dict) else {}
            expected_users = [candidate_user_id] if candidate_user_id is not None else []
            expected_markets = [candidate_market_slug] if candidate_market_slug else []
            results.append(
                CheckResult(
                    "api:/api/account/watchlist/import-local",
                    import_ok
                    and imported_watchlist.get("users", []) == expected_users
                    and imported_watchlist.get("markets", []) == expected_markets,
                    {
                        "status_code": import_response.status_code,
                        "watchlist": imported_watchlist,
                        "imported": (import_result.get("imported") or {}) if isinstance(import_result, dict) else {},
                    },
                )
            )

        bad_login_response = client.post(
            "/api/auth/login",
            json={
                "email": auth_email,
                "password": "wrong-password",
            },
        )
        results.append(
            CheckResult(
                "api:/api/auth/login invalid_credentials",
                bad_login_response.status_code == 401,
                {"status_code": bad_login_response.status_code},
            )
        )

        logout_response = client.post("/api/auth/logout")
        logout_ok = logout_response.status_code == 200
        results.append(
            CheckResult(
                "api:/api/auth/logout",
                logout_ok,
                {
                    "status_code": logout_response.status_code,
                    "cookie_cleared": SESSION_COOKIE_NAME not in logout_response.cookies,
                },
            )
        )

        auth_me_signed_out = client.get("/api/auth/me")
        results.append(
            CheckResult(
                "api:/api/auth/me signed_out",
                auth_me_signed_out.status_code == 401,
                {"status_code": auth_me_signed_out.status_code},
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
            whale_scores = build_whale_score_snapshot(session)
            dashboard = build_dashboard_snapshot(session)
            results.append(
                CheckResult(
                    "dashboard_build",
                    True,
                    {
                        "whale_scores": whale_scores,
                        "result": dashboard,
                    },
                )
            )

    results.extend(
        CheckResult(
            f"history_partition:{item.name}",
            item.ok,
            item.details,
        )
        for item in run_history_partition_checks("")
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
