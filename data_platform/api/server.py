"""Internal read-only FastAPI surface for the data platform."""

from __future__ import annotations

import copy
from threading import Lock
from time import monotonic
from typing import Any
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from data_platform.db.session import session_scope
from data_platform.models import AppAccount
from data_platform.models.base import utc_now
from data_platform.services.account_auth import (
    ACCOUNT_ROLE_ADMIN,
    ACCOUNT_ROLE_MODERATOR,
    DuplicateEmailError,
    SESSION_COOKIE_NAME,
    SESSION_DURATION,
    authenticate_account,
    create_account,
    create_account_session,
    destroy_account_session,
    follow_market,
    follow_user,
    import_watchlist,
    normalize_account_role,
    normalize_display_name,
    resolve_account_session,
    role_meets_threshold,
    serialize_account_session,
    unfollow_market,
    unfollow_user,
    update_account_preferences,
)
from data_platform.services.read_api import (
    database_health,
    following_dashboard,
    following_overview,
    home_summary,
    market_whale_concentration,
    recent_whale_entries,
    latest_dashboard_snapshot,
    latest_dashboard_markets,
    latest_leaderboard,
    latest_market_profile,
    latest_scrape_run,
    latest_user_whale_profile,
    latest_whale_scores,
    list_markets,
    list_positions,
    list_transactions,
    list_users,
    top_profitable_resolved_users,
    user_activity_insights,
    whale_entry_behavior,
)
from data_platform.settings import get_settings

app = FastAPI(
    title="Whaling Data Platform API",
    version="0.1.0",
    description="Internal API for prediction-market analytics plus local account state.",
)

settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FOLLOWING_DASHBOARD_CACHE_TTL_SECONDS = 20.0
SNAPSHOT_CACHE_TTL_SECONDS = 20.0
LEADERBOARD_CACHE_TTL_SECONDS = 20.0
HOME_SUMMARY_CACHE_TTL_SECONDS = 20.0
ANALYTICS_CACHE_TTL_SECONDS = 20.0
LATEST_WHALES_CACHE_TTL_SECONDS = 20.0
MARKET_PROFILE_CACHE_TTL_SECONDS = 30.0
USER_WHALE_PROFILE_CACHE_TTL_SECONDS = 30.0
USER_ACTIVITY_INSIGHTS_CACHE_TTL_SECONDS = 20.0
_following_dashboard_cache: dict[tuple[tuple[int, ...], tuple[str, ...]], tuple[float, dict[str, object]]] = {}
_following_dashboard_cache_lock = Lock()
_latest_dashboard_cache: dict[str, tuple[float, dict[str, object]]] = {}
_latest_dashboard_cache_lock = Lock()
_latest_dashboard_markets_cache: dict[int, tuple[float, dict[str, object]]] = {}
_latest_dashboard_markets_cache_lock = Lock()
_latest_leaderboard_cache: dict[str, tuple[float, dict[str, object]]] = {}
_latest_leaderboard_cache_lock = Lock()
_home_summary_cache: dict[str, tuple[float, dict[str, object]]] = {}
_home_summary_cache_lock = Lock()
_top_profitable_users_cache: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}
_top_profitable_users_cache_lock = Lock()
_market_whale_concentration_cache: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}
_market_whale_concentration_cache_lock = Lock()
_whale_entry_behavior_cache: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}
_whale_entry_behavior_cache_lock = Lock()
_recent_whale_entries_cache: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}
_recent_whale_entries_cache_lock = Lock()
_latest_whales_cache: dict[tuple[int, bool, bool, str], tuple[float, dict[str, object]]] = {}
_latest_whales_cache_lock = Lock()
_market_profile_cache: dict[str, tuple[float, dict[str, object]]] = {}
_market_profile_cache_lock = Lock()
_user_whale_profile_cache: dict[int, tuple[float, dict[str, object]]] = {}
_user_whale_profile_cache_lock = Lock()
_user_activity_insights_cache: dict[tuple[int, str], tuple[float, dict[str, object]]] = {}
_user_activity_insights_cache_lock = Lock()


def _service_error(exc: Exception) -> HTTPException:
    """Convert backend exceptions into consistent API errors."""
    return HTTPException(status_code=503, detail=f"Database service unavailable: {exc}")


class FollowingOverviewRequest(BaseModel):
    """Request payload for watchlist-driven Following page analytics."""

    user_ids: list[int] = Field(default_factory=list)
    market_slugs: list[str] = Field(default_factory=list)


class SignUpRequest(BaseModel):
    """Request payload for self-serve account creation."""

    display_name: str = Field(min_length=1, max_length=80)
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    """Request payload for app-account sign-in."""

    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class LocalWatchlistImportRequest(BaseModel):
    """Request payload for importing the legacy browser-local watchlist."""

    user_ids: list[int] = Field(default_factory=list)
    market_slugs: list[str] = Field(default_factory=list)


class HomepagePreferencesPatch(BaseModel):
    """Partial homepage preference patch."""

    research_timeframe: Literal["7d", "30d", "90d", "all"] | None = None


class UserProfilePreferencesPatch(BaseModel):
    """Partial user-profile preference patch."""

    analytics_timeframe: Literal["7d", "30d", "90d", "all"] | None = None


class LeaderboardUserFiltersPatch(BaseModel):
    """Partial leaderboard user-filter preference patch."""

    board: Literal["all", "trusted", "whale", "potential", "standard"] | None = None
    platform: Literal["all", "polymarket"] | None = None
    min_trades: int | None = Field(default=None, ge=0)
    sort: Literal["trust", "profitability", "trades"] | None = None


class LeaderboardMarketFiltersPatch(BaseModel):
    """Partial leaderboard market-filter preference patch."""

    min_whales: int | None = Field(default=None, ge=0)
    sort: Literal["trusted", "whales", "volume"] | None = None


class LeaderboardPreferencesPatch(BaseModel):
    """Partial leaderboard preference patch."""

    active_board: Literal["market", "user"] | None = None
    user_filters: LeaderboardUserFiltersPatch | None = None
    market_filters: LeaderboardMarketFiltersPatch | None = None


class AccountPreferencesPatchRequest(BaseModel):
    """Partial account preference patch."""

    homepage: HomepagePreferencesPatch | None = None
    user_profile: UserProfilePreferencesPatch | None = None
    leaderboard: LeaderboardPreferencesPatch | None = None


class AdminAccountPatchRequest(BaseModel):
    """Partial admin patch for one app account."""

    display_name: str | None = Field(default=None, min_length=1, max_length=80)
    role: Literal["viewer", "moderator", "admin"] | None = None
    is_active: bool | None = None


def _normalize_following_user_ids(values: list[int] | None) -> tuple[int, ...]:
    """Return stable unique positive ids for Following cache keys."""
    seen: set[int] = set()
    normalized: list[int] = []
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed <= 0 or parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return tuple(normalized)


def _normalize_following_market_slugs(values: list[str] | None) -> tuple[str, ...]:
    """Return stable unique lowercase slugs for Following cache keys."""
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or []:
        if not isinstance(value, str):
            continue
        slug = value.strip().lower()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        normalized.append(slug)
    return tuple(normalized)


def _following_dashboard_cache_key(payload: FollowingOverviewRequest) -> tuple[tuple[int, ...], tuple[str, ...]]:
    """Build the cache key for one Following dashboard request."""
    return (
        _normalize_following_user_ids(payload.user_ids),
        _normalize_following_market_slugs(payload.market_slugs),
    )


def _normalize_timeframe(value: str) -> str:
    """Normalize a timeframe query value for caching and downstream reads."""
    return value.strip().lower()


def _cache_get(
    cache: dict[Any, tuple[float, dict[str, object]]],
    *,
    cache_key: Any,
    ttl_seconds: float,
    lock: Lock,
) -> dict[str, object] | None:
    """Return a deep-copied cached payload when the TTL is still valid."""
    with lock:
        cached = cache.get(cache_key)
        if cached is None:
            return None
        cached_at, cached_payload = cached
        if (monotonic() - cached_at) >= ttl_seconds:
            cache.pop(cache_key, None)
            return None
        return copy.deepcopy(cached_payload)


def _cache_set(
    cache: dict[Any, tuple[float, dict[str, object]]],
    *,
    cache_key: Any,
    payload: dict[str, object],
    lock: Lock,
) -> None:
    """Store a deep-copied API payload in the in-process TTL cache."""
    with lock:
        cache[cache_key] = (monotonic(), copy.deepcopy(payload))


def _set_session_cookie(response: Response, token: str) -> None:
    """Attach the persistent auth cookie to a response."""
    max_age = int(SESSION_DURATION.total_seconds())
    cookie_kwargs: dict[str, Any] = {
        "max_age": max_age,
        "httponly": True,
        "samesite": settings.session_cookie_samesite,
        "secure": settings.session_cookie_secure,
        "path": "/",
    }
    if settings.session_cookie_domain:
        cookie_kwargs["domain"] = settings.session_cookie_domain
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        **cookie_kwargs,
    )


def _clear_session_cookie(response: Response) -> None:
    """Delete the auth cookie from a response."""
    cookie_kwargs: dict[str, Any] = {
        "httponly": True,
        "samesite": settings.session_cookie_samesite,
        "secure": settings.session_cookie_secure,
        "path": "/",
    }
    if settings.session_cookie_domain:
        cookie_kwargs["domain"] = settings.session_cookie_domain
    response.delete_cookie(key=SESSION_COOKIE_NAME, **cookie_kwargs)


def _require_account(session: object, request: Request) -> AppAccount:
    """Resolve the signed-in app account from the auth cookie or raise 401."""
    resolved = resolve_account_session(session, request.cookies.get(SESSION_COOKIE_NAME))
    if resolved is None:
        raise HTTPException(status_code=401, detail="Authentication required.")
    account, _session = resolved
    return account


def _require_role(session: object, request: Request, minimum_role: str) -> AppAccount:
    """Resolve the signed-in account and enforce a minimum role."""
    account = _require_account(session, request)
    if not role_meets_threshold(account.role, minimum_role):
        raise HTTPException(status_code=403, detail="Insufficient permissions.")
    return account


def _serialize_admin_account(account: AppAccount) -> dict[str, object]:
    """Return the admin-facing account metadata payload."""
    return {
        "account_id": account.account_id,
        "email": account.email,
        "display_name": account.display_name,
        "role": account.role,
        "is_active": account.is_active,
        "created_at": account.created_at.isoformat() if account.created_at else None,
        "updated_at": account.updated_at.isoformat() if account.updated_at else None,
        "last_login_at": account.last_login_at.isoformat() if account.last_login_at else None,
    }


@app.get("/")
async def root() -> dict[str, str]:
    """Return a short API summary."""
    return {
        "message": "Whaling Data Platform API",
        "health_endpoint": "/health",
        "status_endpoint": "/api/status/ingestion",
    }


@app.post("/api/auth/signup")
async def post_auth_signup(payload: SignUpRequest, response: Response) -> dict[str, object]:
    """Create a new app account and immediately sign it in."""
    try:
        with session_scope() as session:
            account = create_account(
                session,
                email=payload.email,
                password=payload.password,
                display_name=payload.display_name,
            )
            token = create_account_session(session, account)
            session_payload = serialize_account_session(session, account)
        _set_session_cookie(response, token)
        return {"session": session_payload}
    except DuplicateEmailError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/auth/login")
async def post_auth_login(payload: LoginRequest, response: Response) -> dict[str, object]:
    """Authenticate an existing app account and issue a cookie-backed session."""
    try:
        with session_scope() as session:
            account = authenticate_account(session, email=payload.email, password=payload.password)
            if account is None:
                raise HTTPException(status_code=401, detail="Invalid email or password.")
            token = create_account_session(session, account)
            session_payload = serialize_account_session(session, account)
        _set_session_cookie(response, token)
        return {"session": session_payload}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/auth/logout")
async def post_auth_logout(request: Request, response: Response) -> dict[str, bool]:
    """Invalidate the current auth session and clear the cookie."""
    try:
        with session_scope() as session:
            destroy_account_session(session, request.cookies.get(SESSION_COOKIE_NAME))
        _clear_session_cookie(response)
        return {"ok": True}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/auth/me")
async def get_auth_me(request: Request) -> dict[str, object]:
    """Return the signed-in account payload for frontend bootstrap."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return {"session": serialize_account_session(session, account)}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/account/follow/users/{user_id}")
async def post_follow_user(user_id: int, request: Request) -> dict[str, object]:
    """Add a trader to the signed-in account watchlist."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return {"watchlist": follow_user(session, account_id=account.account_id, user_id=user_id)}
    except HTTPException:
        raise
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.delete("/api/account/follow/users/{user_id}")
async def delete_follow_user(user_id: int, request: Request) -> dict[str, object]:
    """Remove a trader from the signed-in account watchlist."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return {"watchlist": unfollow_user(session, account_id=account.account_id, user_id=user_id)}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/account/follow/markets/{market_slug}")
async def post_follow_market(market_slug: str, request: Request) -> dict[str, object]:
    """Add a market to the signed-in account watchlist."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return {"watchlist": follow_market(session, account_id=account.account_id, market_slug=market_slug)}
    except HTTPException:
        raise
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.delete("/api/account/follow/markets/{market_slug}")
async def delete_follow_market(market_slug: str, request: Request) -> dict[str, object]:
    """Remove a market from the signed-in account watchlist."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return {"watchlist": unfollow_market(session, account_id=account.account_id, market_slug=market_slug)}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.patch("/api/account/preferences")
async def patch_account_preferences(payload: AccountPreferencesPatchRequest, request: Request) -> dict[str, object]:
    """Persist validated stable UI preferences for the signed-in account."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            preferences = update_account_preferences(
                session,
                account_id=account.account_id,
                patch=payload.model_dump(exclude_none=True),
            )
            return {"preferences": preferences}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/account/watchlist/import-local")
async def post_import_local_watchlist(payload: LocalWatchlistImportRequest, request: Request) -> dict[str, object]:
    """Merge the legacy browser-local watchlist into the signed-in account."""
    try:
        with session_scope() as session:
            account = _require_account(session, request)
            return import_watchlist(
                session,
                account_id=account.account_id,
                user_ids=payload.user_ids,
                market_slugs=payload.market_slugs,
            )
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/admin/session")
async def get_admin_session(request: Request) -> dict[str, object]:
    """Return the signed-in session only when the account has moderator access."""
    try:
        with session_scope() as session:
            account = _require_role(session, request, ACCOUNT_ROLE_MODERATOR)
            return {"session": serialize_account_session(session, account)}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/admin/accounts")
async def get_admin_accounts(request: Request) -> dict[str, object]:
    """Return app-account metadata for moderator/admin account management UIs."""
    try:
        with session_scope() as session:
            _require_role(session, request, ACCOUNT_ROLE_MODERATOR)
            accounts = session.execute(
                select(AppAccount).order_by(AppAccount.created_at.desc(), AppAccount.account_id.desc())
            ).scalars().all()
            return {"items": [_serialize_admin_account(account) for account in accounts]}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.patch("/api/admin/accounts/{account_id}")
async def patch_admin_account(
    account_id: int,
    payload: AdminAccountPatchRequest,
    request: Request,
) -> dict[str, object]:
    """Update account role, activation state, or display name with admin privileges."""
    try:
        with session_scope() as session:
            caller = _require_role(session, request, ACCOUNT_ROLE_ADMIN)
            account = session.get(AppAccount, account_id)
            if account is None:
                raise HTTPException(status_code=404, detail=f"Account {account_id} not found.")
            if account.account_id == caller.account_id:
                if payload.is_active is False:
                    raise HTTPException(status_code=400, detail="You cannot deactivate your own admin account.")
                if payload.role is not None and not role_meets_threshold(payload.role, ACCOUNT_ROLE_ADMIN):
                    raise HTTPException(status_code=400, detail="You cannot demote your own admin account.")

            if payload.display_name is not None:
                account.display_name = normalize_display_name(payload.display_name)
            if payload.role is not None:
                account.role = normalize_account_role(payload.role)
            if payload.is_active is not None:
                account.is_active = payload.is_active
            if payload.model_dump(exclude_none=True):
                account.updated_at = utc_now()
            return {"account": _serialize_admin_account(account)}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/health")
async def health() -> dict[str, str]:
    """Return application and database health."""
    try:
        with session_scope() as session:
            database_health(session)
        return {"status": "ok", "database": "ok"}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/status/ingestion")
async def ingestion_status() -> dict[str, object | None]:
    """Return the latest scrape-run status."""
    try:
        with session_scope() as session:
            return {"latest_scrape_run": latest_scrape_run(session)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/markets")
async def get_markets(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent market rows from the normalized source layer."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_markets(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users")
async def get_users(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent user rows from the normalized source layer."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_users(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/transactions")
async def get_transactions(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent normalized transaction rows."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_transactions(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/positions")
async def get_positions(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent normalized position snapshots."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_positions(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/leaderboards/latest")
def get_latest_leaderboard() -> dict[str, object | None]:
    """Return the latest derived leaderboard snapshot, if present."""
    cached_response = _cache_get(
        _latest_leaderboard_cache,
        cache_key="all",
        ttl_seconds=LEADERBOARD_CACHE_TTL_SECONDS,
        lock=_latest_leaderboard_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {"leaderboard": latest_leaderboard(session)}
        _cache_set(
            _latest_leaderboard_cache,
            cache_key="all",
            payload=response_payload,
            lock=_latest_leaderboard_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/leaderboards/trusted/latest")
def get_latest_trusted_leaderboard() -> dict[str, object | None]:
    """Return the latest trusted-whale leaderboard rows, if present."""
    cached_response = _cache_get(
        _latest_leaderboard_cache,
        cache_key="internal_trusted",
        ttl_seconds=LEADERBOARD_CACHE_TTL_SECONDS,
        lock=_latest_leaderboard_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {"leaderboard": latest_leaderboard(session, board_type="internal_trusted")}
        _cache_set(
            _latest_leaderboard_cache,
            cache_key="internal_trusted",
            payload=response_payload,
            lock=_latest_leaderboard_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/dashboards/latest")
def get_latest_dashboard() -> dict[str, object | None]:
    """Return the latest derived dashboard snapshot summary, if present."""
    cached_response = _cache_get(
        _latest_dashboard_cache,
        cache_key="latest",
        ttl_seconds=SNAPSHOT_CACHE_TTL_SECONDS,
        lock=_latest_dashboard_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {"dashboard": latest_dashboard_snapshot(session)}
        _cache_set(
            _latest_dashboard_cache,
            cache_key="latest",
            payload=response_payload,
            lock=_latest_dashboard_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/dashboards/latest/markets")
def get_latest_dashboard_markets(limit: int = Query(50, ge=1, le=250)) -> dict[str, object | None]:
    """Return the latest dashboard-market rows for frontend leaderboard use."""
    cache_key = int(limit)
    cached_response = _cache_get(
        _latest_dashboard_markets_cache,
        cache_key=cache_key,
        ttl_seconds=SNAPSHOT_CACHE_TTL_SECONDS,
        lock=_latest_dashboard_markets_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {"markets": latest_dashboard_markets(session, limit=limit)}
        _cache_set(
            _latest_dashboard_markets_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_latest_dashboard_markets_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/home/summary")
def get_home_summary() -> dict[str, object]:
    """Return homepage summary cards backed by the latest analytics state."""
    cached_response = _cache_get(
        _home_summary_cache,
        cache_key="home",
        ttl_seconds=HOME_SUMMARY_CACHE_TTL_SECONDS,
        lock=_home_summary_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {"summary": home_summary(session)}
        _cache_set(
            _home_summary_cache,
            cache_key="home",
            payload=response_payload,
            lock=_home_summary_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/top-profitable-users")
def get_top_profitable_users(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object]:
    """Return the top Polymarket users by conservative resolved-market profitability."""
    normalized_timeframe = _normalize_timeframe(timeframe)
    cache_key = (int(limit), normalized_timeframe)
    cached_response = _cache_get(
        _top_profitable_users_cache,
        cache_key=cache_key,
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        lock=_top_profitable_users_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {
                "analytics": top_profitable_resolved_users(
                    session,
                    limit=limit,
                    timeframe=normalized_timeframe,
                )
            }
        _cache_set(
            _top_profitable_users_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_top_profitable_users_cache_lock,
        )
        return response_payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/market-whale-concentration")
def get_market_whale_concentration(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object | None]:
    """Return the latest cross-platform market concentration ranking."""
    normalized_timeframe = _normalize_timeframe(timeframe)
    cache_key = (int(limit), normalized_timeframe)
    cached_response = _cache_get(
        _market_whale_concentration_cache,
        cache_key=cache_key,
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        lock=_market_whale_concentration_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {
                "analytics": market_whale_concentration(
                    session,
                    limit=limit,
                    timeframe=normalized_timeframe,
                )
            }
        _cache_set(
            _market_whale_concentration_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_market_whale_concentration_cache_lock,
        )
        return response_payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/whale-entry-behavior")
def get_whale_entry_behavior(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object | None]:
    """Return Polymarket whale entry-price behavior for the selected timeframe."""
    normalized_timeframe = _normalize_timeframe(timeframe)
    cache_key = (int(limit), normalized_timeframe)
    cached_response = _cache_get(
        _whale_entry_behavior_cache,
        cache_key=cache_key,
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        lock=_whale_entry_behavior_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {
                "analytics": whale_entry_behavior(
                    session,
                    limit=limit,
                    timeframe=normalized_timeframe,
                )
            }
        _cache_set(
            _whale_entry_behavior_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_whale_entry_behavior_cache_lock,
        )
        return response_payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/recent-whale-entries")
def get_recent_whale_entries(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object | None]:
    """Return the most recent whale buy-entry markets for the selected timeframe."""
    normalized_timeframe = _normalize_timeframe(timeframe)
    cache_key = (int(limit), normalized_timeframe)
    cached_response = _cache_get(
        _recent_whale_entries_cache,
        cache_key=cache_key,
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        lock=_recent_whale_entries_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {
                "analytics": recent_whale_entries(
                    session,
                    limit=limit,
                    timeframe=normalized_timeframe,
                )
            }
        _cache_set(
            _recent_whale_entries_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_recent_whale_entries_cache_lock,
        )
        return response_payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/whales/latest")
def get_latest_whales(
    limit: int = Query(50, ge=1, le=250),
    whales_only: bool = Query(False),
    trusted_only: bool = Query(False),
    tier: Literal["all", "trusted", "whale", "potential", "standard"] = Query("all"),
) -> dict[str, object | None]:
    """Return the latest whale-score rows with optional whale/trusted filters."""
    normalized_tier = tier.strip().lower()
    cache_key = (int(limit), bool(whales_only), bool(trusted_only), normalized_tier)
    cached_response = _cache_get(
        _latest_whales_cache,
        cache_key=cache_key,
        ttl_seconds=LATEST_WHALES_CACHE_TTL_SECONDS,
        lock=_latest_whales_cache_lock,
    )
    if cached_response is not None:
        return cached_response

    try:
        with session_scope() as session:
            response_payload = {
                "whales": latest_whale_scores(
                    session,
                    limit=limit,
                    whales_only=whales_only,
                    trusted_only=trusted_only,
                    tier=normalized_tier,
                )
            }
        _cache_set(
            _latest_whales_cache,
            cache_key=cache_key,
            payload=response_payload,
            lock=_latest_whales_cache_lock,
        )
        return response_payload
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/following/overview")
def post_following_overview(payload: FollowingOverviewRequest) -> dict[str, object]:
    """Return watchlist aggregate analytics for the Following page."""
    try:
        with session_scope() as session:
            return {
                "overview": following_overview(
                    session,
                    user_ids=payload.user_ids,
                    market_slugs=payload.market_slugs,
                )
            }
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.post("/api/following/dashboard")
def post_following_dashboard(payload: FollowingOverviewRequest) -> dict[str, object]:
    """Return the complete Following page payload in one backend call."""
    cache_key = _following_dashboard_cache_key(payload)
    cached_payload = _cache_get(
        _following_dashboard_cache,
        cache_key=cache_key,
        ttl_seconds=FOLLOWING_DASHBOARD_CACHE_TTL_SECONDS,
        lock=_following_dashboard_cache_lock,
    )
    if cached_payload is not None:
        return {"dashboard": cached_payload}

    try:
        with session_scope() as session:
            dashboard_payload = following_dashboard(
                session,
                user_ids=payload.user_ids,
                market_slugs=payload.market_slugs,
            )
        _cache_set(
            _following_dashboard_cache,
            cache_key=cache_key,
            payload=dashboard_payload,
            lock=_following_dashboard_cache_lock,
        )
        return {"dashboard": dashboard_payload}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users/{user_id}/whale-profile")
def get_user_whale_profile(user_id: int) -> dict[str, object]:
    """Return whale-specific resolved-performance details for one user."""
    cached_payload = _cache_get(
        _user_whale_profile_cache,
        cache_key=int(user_id),
        ttl_seconds=USER_WHALE_PROFILE_CACHE_TTL_SECONDS,
        lock=_user_whale_profile_cache_lock,
    )
    if cached_payload is not None:
        return {"profile": cached_payload}

    try:
        with session_scope() as session:
            profile = latest_user_whale_profile(session, user_id=user_id)
            if profile is None:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found")
            _cache_set(
                _user_whale_profile_cache,
                cache_key=int(user_id),
                payload=profile,
                lock=_user_whale_profile_cache_lock,
            )
            return {"profile": profile}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users/{user_id}/activity-insights")
def get_user_activity_insights(
    user_id: int,
    timeframe: str = Query("all"),
) -> dict[str, object]:
    """Return user-specific activity insights for the selected timeframe."""
    normalized_timeframe = timeframe.strip().lower()
    cache_key = (int(user_id), normalized_timeframe)
    cached_payload = _cache_get(
        _user_activity_insights_cache,
        cache_key=cache_key,
        ttl_seconds=USER_ACTIVITY_INSIGHTS_CACHE_TTL_SECONDS,
        lock=_user_activity_insights_cache_lock,
    )
    if cached_payload is not None:
        return {"insights": cached_payload}

    try:
        with session_scope() as session:
            insights = user_activity_insights(session, user_id=user_id, timeframe=normalized_timeframe)
            if insights is None:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found")
            _cache_set(
                _user_activity_insights_cache,
                cache_key=cache_key,
                payload=insights,
                lock=_user_activity_insights_cache_lock,
            )
            return {"insights": insights}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/markets/{market_slug}/profile")
def get_market_profile(market_slug: str) -> dict[str, object]:
    """Return latest dashboard-backed market profile details for one market slug."""
    normalized_market_slug = market_slug.strip().lower()
    cached_payload = _cache_get(
        _market_profile_cache,
        cache_key=normalized_market_slug,
        ttl_seconds=MARKET_PROFILE_CACHE_TTL_SECONDS,
        lock=_market_profile_cache_lock,
    )
    if cached_payload is not None:
        return {"profile": cached_payload}

    try:
        with session_scope() as session:
            profile = latest_market_profile(session, market_slug=market_slug)
            if profile is None:
                raise HTTPException(status_code=404, detail=f"Market {market_slug} not found")
            _cache_set(
                _market_profile_cache,
                cache_key=normalized_market_slug,
                payload=profile,
                lock=_market_profile_cache_lock,
            )
            return {"profile": profile}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc
