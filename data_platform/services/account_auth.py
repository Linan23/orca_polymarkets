"""App-account authentication, watchlist, and preference helpers."""

from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import hashlib
import secrets
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from sqlalchemy import delete, desc, func, select
from sqlalchemy.orm import Session

from data_platform.models import (
    AppAccount,
    AppAccountPreferences,
    AppSession,
    AppWatchlistMarket,
    AppWatchlistUser,
    MarketContract,
    UserAccount,
)
from data_platform.models.base import utc_now


SESSION_COOKIE_NAME = "orca_session"
SESSION_DURATION = timedelta(days=30)
_PASSWORD_HASHER = PasswordHasher()

DEFAULT_ACCOUNT_PREFERENCES: dict[str, Any] = {
    "homepage": {
        "research_timeframe": "all",
    },
    "user_profile": {
        "analytics_timeframe": "30d",
    },
    "leaderboard": {
        "active_board": "market",
        "user_filters": {
            "board": "all",
            "platform": "all",
            "min_trades": 0,
            "sort": "trust",
        },
        "market_filters": {
            "min_whales": 0,
            "sort": "trusted",
        },
    },
}


class DuplicateEmailError(ValueError):
    """Raised when a sign-up email already exists."""


def normalize_email(value: str) -> str:
    """Return a normalized account email."""
    return value.strip().lower()


def normalize_display_name(value: str) -> str:
    """Return a trimmed display name."""
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError("Display name is required.")
    return normalized


def normalize_market_slug(value: str) -> str:
    """Return a normalized market slug."""
    return value.strip().lower()


def hash_password(password: str) -> str:
    """Return an Argon2id password hash."""
    return _PASSWORD_HASHER.hash(password)


def verify_password(password_hash: str, password: str) -> bool:
    """Return whether the supplied password matches the Argon2id hash."""
    try:
        return _PASSWORD_HASHER.verify(password_hash, password)
    except (InvalidHashError, VerifyMismatchError):
        return False


def _session_token_hash(token: str) -> str:
    """Return the SHA-256 hash for an opaque session token."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _generate_session_token() -> str:
    """Generate a new opaque session token."""
    return secrets.token_urlsafe(48)


def _merge_dicts(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge a patch into a base dictionary."""
    merged = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_watchlist_state(session: Session, account_id: int) -> dict[str, list[Any]]:
    """Return the saved watchlist for one app account."""
    user_ids = session.execute(
        select(AppWatchlistUser.user_id)
        .where(AppWatchlistUser.account_id == account_id)
        .order_by(desc(AppWatchlistUser.created_at), desc(AppWatchlistUser.user_id))
    ).scalars().all()
    market_slugs = session.execute(
        select(AppWatchlistMarket.market_slug)
        .where(AppWatchlistMarket.account_id == account_id)
        .order_by(desc(AppWatchlistMarket.created_at), AppWatchlistMarket.market_slug)
    ).scalars().all()
    return {
        "users": [int(value) for value in user_ids],
        "markets": [str(value) for value in market_slugs],
    }


def _load_preferences_payload(session: Session, account_id: int) -> dict[str, Any]:
    """Return merged preferences with defaults applied."""
    row = session.scalar(
        select(AppAccountPreferences).where(AppAccountPreferences.account_id == account_id)
    )
    stored = row.preference_payload if row and isinstance(row.preference_payload, dict) else {}
    return _merge_dicts(DEFAULT_ACCOUNT_PREFERENCES, stored)


def _ensure_preferences_row(session: Session, account_id: int) -> AppAccountPreferences:
    """Return the preference row for an account, creating it when absent."""
    row = session.scalar(
        select(AppAccountPreferences).where(AppAccountPreferences.account_id == account_id)
    )
    if row is None:
        row = AppAccountPreferences(
            account_id=account_id,
            preference_payload=deepcopy(DEFAULT_ACCOUNT_PREFERENCES),
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        session.add(row)
        session.flush()
    return row


def serialize_account_session(session: Session, account: AppAccount) -> dict[str, Any]:
    """Return the frontend-facing account/session payload."""
    return {
        "account": {
            "account_id": account.account_id,
            "email": account.email,
            "display_name": account.display_name,
            "created_at": account.created_at.isoformat() if account.created_at else None,
            "last_login_at": account.last_login_at.isoformat() if account.last_login_at else None,
        },
        "watchlist": _load_watchlist_state(session, account.account_id),
        "preferences": _load_preferences_payload(session, account.account_id),
    }


def create_account(session: Session, *, email: str, password: str, display_name: str) -> AppAccount:
    """Create a new app account with an Argon2id password hash."""
    normalized_email = normalize_email(email)
    if session.scalar(select(AppAccount).where(AppAccount.email == normalized_email)) is not None:
        raise DuplicateEmailError("An account with that email already exists.")

    now = utc_now()
    account = AppAccount(
        email=normalized_email,
        password_hash=hash_password(password),
        display_name=normalize_display_name(display_name),
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    session.add(account)
    session.flush()
    _ensure_preferences_row(session, account.account_id)
    return account


def authenticate_account(session: Session, *, email: str, password: str) -> AppAccount | None:
    """Return the matching account when the password is valid."""
    normalized_email = normalize_email(email)
    account = session.scalar(select(AppAccount).where(AppAccount.email == normalized_email))
    if account is None or not account.is_active:
        return None
    if not verify_password(account.password_hash, password):
        return None
    return account


def create_account_session(session: Session, account: AppAccount) -> str:
    """Create a persistent cookie-backed session and return the opaque token."""
    now = utc_now()
    token = _generate_session_token()
    session_row = AppSession(
        account_id=account.account_id,
        session_token_hash=_session_token_hash(token),
        created_at=now,
        expires_at=now + SESSION_DURATION,
        last_seen_at=now,
    )
    account.last_login_at = now
    account.updated_at = now
    session.add(session_row)
    session.flush()
    return token


def resolve_account_session(session: Session, token: str | None) -> tuple[AppAccount, AppSession] | None:
    """Return the account and session rows for a valid cookie token."""
    if not token:
        return None
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None:
        return None
    now = utc_now()
    if session_row.expires_at <= now:
        session.delete(session_row)
        session.flush()
        return None
    account = session.get(AppAccount, session_row.account_id)
    if account is None or not account.is_active:
        session.delete(session_row)
        session.flush()
        return None
    session_row.last_seen_at = now
    return account, session_row


def destroy_account_session(session: Session, token: str | None) -> bool:
    """Delete a persisted session token when it exists."""
    if not token:
        return False
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None:
        return False
    session.delete(session_row)
    session.flush()
    return True


def _valid_watchlist_user_ids(session: Session, user_ids: list[int]) -> list[int]:
    """Return only existing analytics user ids in stable input order."""
    if not user_ids:
        return []
    normalized: list[int] = []
    seen: set[int] = set()
    for value in user_ids:
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    found_ids = {
        int(value)
        for value in session.execute(
            select(UserAccount.user_id).where(UserAccount.user_id.in_(normalized))
        ).scalars()
    }
    return [user_id for user_id in normalized if user_id in found_ids]


def _valid_watchlist_market_slugs(session: Session, market_slugs: list[str]) -> list[str]:
    """Return only existing market slugs in stable input order."""
    if not market_slugs:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in market_slugs:
        value = normalize_market_slug(raw_value)
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        return []
    found_slugs = {
        value
        for value in session.execute(
            select(func.lower(MarketContract.market_slug))
            .where(MarketContract.market_slug.is_not(None))
            .where(func.lower(MarketContract.market_slug).in_(normalized))
        ).scalars()
        if value
    }
    return [value for value in normalized if value in found_slugs]


def follow_user(session: Session, *, account_id: int, user_id: int) -> dict[str, list[Any]]:
    """Add one analytics user to an account watchlist."""
    if session.get(UserAccount, user_id) is None:
        raise LookupError(f"User {user_id} not found.")
    existing = session.get(AppWatchlistUser, {"account_id": account_id, "user_id": user_id})
    if existing is None:
        session.add(AppWatchlistUser(account_id=account_id, user_id=user_id, created_at=utc_now()))
        session.flush()
    return _load_watchlist_state(session, account_id)


def unfollow_user(session: Session, *, account_id: int, user_id: int) -> dict[str, list[Any]]:
    """Remove one analytics user from an account watchlist."""
    existing = session.get(AppWatchlistUser, {"account_id": account_id, "user_id": user_id})
    if existing is not None:
        session.delete(existing)
        session.flush()
    return _load_watchlist_state(session, account_id)


def follow_market(session: Session, *, account_id: int, market_slug: str) -> dict[str, list[Any]]:
    """Add one market slug to an account watchlist."""
    normalized_slug = normalize_market_slug(market_slug)
    if not normalized_slug:
        raise LookupError("Market slug is required.")
    exists = session.execute(
        select(MarketContract.market_contract_id)
        .where(MarketContract.market_slug.is_not(None))
        .where(func.lower(MarketContract.market_slug) == normalized_slug)
        .limit(1)
    ).scalar_one_or_none()
    if exists is None:
        raise LookupError(f"Market {normalized_slug} not found.")
    existing = session.get(AppWatchlistMarket, {"account_id": account_id, "market_slug": normalized_slug})
    if existing is None:
        session.add(AppWatchlistMarket(account_id=account_id, market_slug=normalized_slug, created_at=utc_now()))
        session.flush()
    return _load_watchlist_state(session, account_id)


def unfollow_market(session: Session, *, account_id: int, market_slug: str) -> dict[str, list[Any]]:
    """Remove one market slug from an account watchlist."""
    normalized_slug = normalize_market_slug(market_slug)
    existing = session.get(AppWatchlistMarket, {"account_id": account_id, "market_slug": normalized_slug})
    if existing is not None:
        session.delete(existing)
        session.flush()
    return _load_watchlist_state(session, account_id)


def import_watchlist(
    session: Session,
    *,
    account_id: int,
    user_ids: list[int],
    market_slugs: list[str],
) -> dict[str, Any]:
    """Import a legacy local watchlist into the account-scoped watchlist tables."""
    imported_users = 0
    imported_markets = 0
    for user_id in _valid_watchlist_user_ids(session, user_ids):
        existing = session.get(AppWatchlistUser, {"account_id": account_id, "user_id": user_id})
        if existing is not None:
            continue
        session.add(AppWatchlistUser(account_id=account_id, user_id=user_id, created_at=utc_now()))
        imported_users += 1

    for market_slug in _valid_watchlist_market_slugs(session, market_slugs):
        existing = session.get(AppWatchlistMarket, {"account_id": account_id, "market_slug": market_slug})
        if existing is not None:
            continue
        session.add(AppWatchlistMarket(account_id=account_id, market_slug=market_slug, created_at=utc_now()))
        imported_markets += 1

    session.flush()
    return {
        "watchlist": _load_watchlist_state(session, account_id),
        "imported": {
            "users": imported_users,
            "markets": imported_markets,
        },
    }


def update_account_preferences(session: Session, *, account_id: int, patch: dict[str, Any]) -> dict[str, Any]:
    """Merge and persist a validated preference patch."""
    row = _ensure_preferences_row(session, account_id)
    merged = _merge_dicts(DEFAULT_ACCOUNT_PREFERENCES, row.preference_payload if isinstance(row.preference_payload, dict) else {})
    merged = _merge_dicts(merged, patch)
    row.preference_payload = merged
    row.updated_at = utc_now()
    session.flush()
    return deepcopy(merged)


def purge_expired_sessions(session: Session) -> int:
    """Delete expired persisted sessions and return the count."""
    result = session.execute(
        delete(AppSession).where(AppSession.expires_at <= utc_now())
    )
    return int(result.rowcount or 0)
