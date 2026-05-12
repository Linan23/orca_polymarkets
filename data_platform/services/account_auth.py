"""App-account authentication, watchlist, and preference helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from email.message import EmailMessage
import base64
import hmac
import hashlib
import smtplib
import secrets
import struct
import time
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from cryptography.fernet import Fernet
from sqlalchemy import delete, desc, func, or_, select
from sqlalchemy.orm import Session

from data_platform.models import (
    AppAccount,
    AppAccountPreferences,
    AppAuthAuditLog,
    AppAuthToken,
    AppSession,
    AppWatchlistMarket,
    AppWatchlistUser,
    MarketContract,
    UserAccount,
)
from data_platform.models.base import utc_now
from data_platform.settings import Settings, get_settings


SESSION_COOKIE_NAME = "orca_session"
SESSION_DURATION = timedelta(days=30)
AUTH_TOKEN_DURATION = timedelta(hours=24)
PASSWORD_RESET_DURATION = timedelta(hours=1)
MFA_CHALLENGE_DURATION = timedelta(minutes=5)
LOGIN_LOCKOUT_FAILURES = 5
LOGIN_LOCKOUT_DURATION = timedelta(minutes=15)
RATE_LIMIT_WINDOW = timedelta(minutes=15)
RATE_LIMIT_MAX_EVENTS = 10
_PASSWORD_HASHER = PasswordHasher()
ACCOUNT_ROLE_VIEWER = "viewer"
ACCOUNT_ROLE_MODERATOR = "moderator"
ACCOUNT_ROLE_ADMIN = "admin"
ACCOUNT_ROLE_LEVELS = {
    ACCOUNT_ROLE_VIEWER: 0,
    ACCOUNT_ROLE_MODERATOR: 1,
    ACCOUNT_ROLE_ADMIN: 2,
}

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


@dataclass(frozen=True)
class SessionIssue:
    """Newly issued opaque session token and matching CSRF token."""

    session_token: str
    csrf_token: str


@dataclass(frozen=True)
class AuthEmailResult:
    """Result of an email-token flow."""

    sent: bool
    dev_token: str | None = None
    reason: str | None = None


class DuplicateEmailError(ValueError):
    """Raised when a sign-up email already exists."""


def normalize_account_role(value: str) -> str:
    """Return a normalized account role."""
    normalized = value.strip().lower()
    if normalized not in ACCOUNT_ROLE_LEVELS:
        allowed = ", ".join(ACCOUNT_ROLE_LEVELS)
        raise ValueError(f"Unsupported account role '{value}'. Expected one of: {allowed}.")
    return normalized


def role_meets_threshold(role: str, minimum_role: str) -> bool:
    """Return whether one role satisfies another role's minimum privilege."""
    return ACCOUNT_ROLE_LEVELS[normalize_account_role(role)] >= ACCOUNT_ROLE_LEVELS[normalize_account_role(minimum_role)]


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


def hash_token(token: str) -> str:
    """Return the storage hash for an auth token."""
    return _session_token_hash(token)


def _generate_session_token() -> str:
    """Generate a new opaque session token."""
    return secrets.token_urlsafe(48)


def generate_public_token() -> str:
    """Generate an opaque public token for verification/reset/MFA flows."""
    return secrets.token_urlsafe(40)


def _hash_context_value(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def ip_prefix(value: str | None) -> str | None:
    """Return a coarse IP prefix for audit/rate-limit hashing."""
    if not value:
        return None
    if ":" in value:
        return ":".join(value.split(":")[:4])
    parts = value.split(".")
    return ".".join(parts[:3]) if len(parts) >= 3 else value


def client_context(*, ip_address: str | None, user_agent: str | None) -> dict[str, str | None]:
    """Return hashed request context for audit records and sessions."""
    return {
        "ip_prefix_hash": _hash_context_value(ip_prefix(ip_address)),
        "user_agent_hash": _hash_context_value(user_agent),
    }


def signup_domain_allowed(email: str, settings: Settings | None = None) -> bool:
    """Return whether an email can self-register."""
    settings = settings or get_settings()
    domain = normalize_email(email).rsplit("@", 1)[-1] if "@" in normalize_email(email) else ""
    allowed_domains = settings.allowed_signup_email_domains
    if not allowed_domains:
        return settings.app_env.lower() != "production"
    return domain in allowed_domains


def auth_secret(settings: Settings | None = None) -> str:
    """Return the configured auth secret or a development-only fallback."""
    settings = settings or get_settings()
    if settings.auth_secret_key:
        return settings.auth_secret_key
    if settings.app_env.lower() == "production":
        raise RuntimeError("AUTH_SECRET_KEY is required in production.")
    return "orca-dev-insecure-auth-secret"


def _fernet(settings: Settings | None = None) -> Fernet:
    digest = hashlib.sha256(auth_secret(settings).encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_secret(value: str, settings: Settings | None = None) -> str:
    """Encrypt a sensitive account secret for database storage."""
    return _fernet(settings).encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str, settings: Settings | None = None) -> str:
    """Decrypt a sensitive account secret from database storage."""
    return _fernet(settings).decrypt(value.encode("utf-8")).decode("utf-8")


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
    requires_mfa = role_meets_threshold(account.role, ACCOUNT_ROLE_MODERATOR)
    mfa_enabled = account.mfa_enabled_at is not None
    return {
        "account": {
            "account_id": account.account_id,
            "email": account.email,
            "display_name": account.display_name,
            "role": account.role,
            "email_verified": account.email_verified_at is not None,
            "mfa_enabled": mfa_enabled,
            "created_at": account.created_at.isoformat() if account.created_at else None,
            "last_login_at": account.last_login_at.isoformat() if account.last_login_at else None,
        },
        "security_state": {
            "email_verified": account.email_verified_at is not None,
            "mfa_required": requires_mfa,
            "mfa_enabled": mfa_enabled,
            "mfa_setup_required": requires_mfa and not mfa_enabled,
        },
        "watchlist": _load_watchlist_state(session, account.account_id),
        "preferences": _load_preferences_payload(session, account.account_id),
    }


def record_auth_event(
    session: Session,
    *,
    event_type: str,
    account: AppAccount | None = None,
    email: str | None = None,
    context: dict[str, str | None] | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Persist one auth/security audit event."""
    session.add(
        AppAuthAuditLog(
            account_id=account.account_id if account else None,
            email=normalize_email(email or account.email) if (email or account) else None,
            event_type=event_type,
            ip_prefix_hash=(context or {}).get("ip_prefix_hash"),
            user_agent_hash=(context or {}).get("user_agent_hash"),
            details_payload=details or {},
            created_at=utc_now(),
        )
    )


def _recent_auth_event_count(
    session: Session,
    *,
    event_types: tuple[str, ...],
    email: str | None,
    context: dict[str, str | None] | None,
) -> int:
    since = utc_now() - RATE_LIMIT_WINDOW
    predicates = [AppAuthAuditLog.created_at >= since, AppAuthAuditLog.event_type.in_(event_types)]
    scoped = []
    normalized_email = normalize_email(email) if email else None
    if normalized_email:
        scoped.append(AppAuthAuditLog.email == normalized_email)
    ip_hash = (context or {}).get("ip_prefix_hash")
    if ip_hash:
        scoped.append(AppAuthAuditLog.ip_prefix_hash == ip_hash)
    if scoped:
        predicates.append(scoped[0] if len(scoped) == 1 else or_(*scoped))
    return int(session.scalar(select(func.count()).select_from(AppAuthAuditLog).where(*predicates)) or 0)


def rate_limit_exceeded(
    session: Session,
    *,
    event_types: tuple[str, ...],
    email: str | None,
    context: dict[str, str | None] | None,
    max_events: int = RATE_LIMIT_MAX_EVENTS,
) -> bool:
    """Return whether recent auth events exceed a small abuse threshold."""
    return _recent_auth_event_count(session, event_types=event_types, email=email, context=context) >= max_events


def issue_auth_token(
    session: Session,
    *,
    account: AppAccount,
    token_type: str,
    duration: timedelta,
    email: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    """Persist and return a one-time auth token."""
    token = generate_public_token()
    session.add(
        AppAuthToken(
            account_id=account.account_id,
            email=normalize_email(email or account.email),
            token_hash=hash_token(token),
            token_type=token_type,
            metadata_payload=metadata or {},
            expires_at=utc_now() + duration,
            created_at=utc_now(),
        )
    )
    session.flush()
    return token


def consume_auth_token(session: Session, *, token: str, token_type: str) -> AppAuthToken | None:
    """Return and mark a valid one-time auth token as used."""
    row = session.scalar(
        select(AppAuthToken)
        .where(AppAuthToken.token_hash == hash_token(token))
        .where(AppAuthToken.token_type == token_type)
    )
    now = utc_now()
    if row is None or row.used_at is not None or row.expires_at <= now:
        return None
    row.used_at = now
    session.flush()
    return row


def send_auth_email(*, to_email: str, subject: str, body: str, settings: Settings | None = None) -> AuthEmailResult:
    """Send an auth email through SMTP, or return a dev-mode no-SMTP result."""
    settings = settings or get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        return AuthEmailResult(sent=False, reason="smtp_not_configured")
    message = EmailMessage()
    message["From"] = settings.smtp_from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)
    return AuthEmailResult(sent=True)


def send_verification_email(*, account: AppAccount, token: str, settings: Settings | None = None) -> AuthEmailResult:
    """Send or dev-return an email verification link."""
    settings = settings or get_settings()
    verify_url = f"{settings.frontend_origin.rstrip('/')}/login?verify={token}"
    result = send_auth_email(
        to_email=account.email,
        subject="Verify your Orca dashboard account",
        body=f"Verify your Orca dashboard account by opening this link:\n\n{verify_url}\n\nThis link expires in 24 hours.",
        settings=settings,
    )
    if not result.sent and settings.app_env.lower() != "production":
        return AuthEmailResult(sent=False, dev_token=token, reason=result.reason)
    return result


def send_password_reset_email(*, account: AppAccount, token: str, settings: Settings | None = None) -> AuthEmailResult:
    """Send or dev-return a password reset link."""
    settings = settings or get_settings()
    reset_url = f"{settings.frontend_origin.rstrip('/')}/login?reset={token}"
    result = send_auth_email(
        to_email=account.email,
        subject="Reset your Orca dashboard password",
        body=f"Reset your Orca dashboard password by opening this link:\n\n{reset_url}\n\nThis link expires in 1 hour.",
        settings=settings,
    )
    if not result.sent and settings.app_env.lower() != "production":
        return AuthEmailResult(sent=False, dev_token=token, reason=result.reason)
    return result


def create_account(
    session: Session,
    *,
    email: str,
    password: str,
    display_name: str,
    require_email_verification: bool = True,
) -> AppAccount:
    """Create a new app account with an Argon2id password hash."""
    normalized_email = normalize_email(email)
    if session.scalar(select(AppAccount).where(AppAccount.email == normalized_email)) is not None:
        raise DuplicateEmailError("An account with that email already exists.")

    now = utc_now()
    account = AppAccount(
        email=normalized_email,
        password_hash=hash_password(password),
        display_name=normalize_display_name(display_name),
        role=ACCOUNT_ROLE_VIEWER,
        is_active=not require_email_verification,
        email_verified_at=None if require_email_verification else now,
        password_changed_at=now,
        created_at=now,
        updated_at=now,
    )
    session.add(account)
    session.flush()
    _ensure_preferences_row(session, account.account_id)
    return account


def account_locked(account: AppAccount) -> bool:
    """Return whether account login is temporarily locked."""
    return bool(account.locked_until and account.locked_until > utc_now())


def authenticate_account(
    session: Session,
    *,
    email: str,
    password: str,
    context: dict[str, str | None] | None = None,
) -> AppAccount | None:
    """Return the matching account when the password is valid."""
    normalized_email = normalize_email(email)
    account = session.scalar(select(AppAccount).where(AppAccount.email == normalized_email))
    if account is None:
        record_auth_event(session, event_type="login_failed", email=normalized_email, context=context, details={"reason": "unknown_account"})
        return None
    if account_locked(account):
        record_auth_event(session, event_type="login_failed", account=account, context=context, details={"reason": "locked"})
        return None
    if not account.is_active or account.email_verified_at is None:
        record_auth_event(session, event_type="login_failed", account=account, context=context, details={"reason": "inactive_or_unverified"})
        return None
    if not verify_password(account.password_hash, password):
        account.failed_login_count = int(account.failed_login_count or 0) + 1
        if account.failed_login_count >= LOGIN_LOCKOUT_FAILURES:
            account.locked_until = utc_now() + LOGIN_LOCKOUT_DURATION
            record_auth_event(session, event_type="account_locked", account=account, context=context)
        record_auth_event(session, event_type="login_failed", account=account, context=context, details={"reason": "bad_password"})
        return None
    account.failed_login_count = 0
    account.locked_until = None
    record_auth_event(session, event_type="login_success_password", account=account, context=context)
    return account


def create_account_session(
    session: Session,
    account: AppAccount,
    *,
    context: dict[str, str | None] | None = None,
    mfa_verified: bool = False,
) -> SessionIssue:
    """Create a persistent cookie-backed session and return the opaque token."""
    now = utc_now()
    token = _generate_session_token()
    csrf_token = generate_public_token()
    session_row = AppSession(
        account_id=account.account_id,
        session_token_hash=_session_token_hash(token),
        csrf_token_hash=hash_token(csrf_token),
        user_agent_hash=(context or {}).get("user_agent_hash"),
        ip_prefix_hash=(context or {}).get("ip_prefix_hash"),
        mfa_verified_at=now if mfa_verified else None,
        created_at=now,
        expires_at=now + SESSION_DURATION,
        last_seen_at=now,
    )
    account.last_login_at = now
    account.updated_at = now
    session.add(session_row)
    session.flush()
    record_auth_event(session, event_type="session_created", account=account, context=context)
    return SessionIssue(session_token=token, csrf_token=csrf_token)


def resolve_account_session(session: Session, token: str | None) -> tuple[AppAccount, AppSession] | None:
    """Return the account and session rows for a valid cookie token."""
    if not token:
        return None
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None or session_row.revoked_at is not None:
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


def rotate_session_csrf(session: Session, token: str | None) -> str | None:
    """Issue a fresh CSRF token for an existing session token."""
    if not token:
        return None
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None or session_row.revoked_at is not None or session_row.expires_at <= utc_now():
        return None
    csrf_token = generate_public_token()
    session_row.csrf_token_hash = hash_token(csrf_token)
    session_row.last_seen_at = utc_now()
    session.flush()
    return csrf_token


def validate_session_csrf(session: Session, token: str | None, csrf_token: str | None) -> bool:
    """Return whether a submitted CSRF token matches the stored session token."""
    if not token or not csrf_token:
        return False
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None or session_row.revoked_at is not None or session_row.expires_at <= utc_now() or not session_row.csrf_token_hash:
        return False
    return hmac.compare_digest(session_row.csrf_token_hash, hash_token(csrf_token))


def destroy_account_session(session: Session, token: str | None) -> bool:
    """Delete a persisted session token when it exists."""
    if not token:
        return False
    session_row = session.scalar(
        select(AppSession).where(AppSession.session_token_hash == _session_token_hash(token))
    )
    if session_row is None:
        return False
    session_row.revoked_at = utc_now()
    session.flush()
    return True


def revoke_account_sessions(session: Session, account_id: int) -> int:
    """Revoke all sessions for one account."""
    rows = session.execute(
        select(AppSession)
        .where(AppSession.account_id == account_id)
        .where(AppSession.revoked_at.is_(None))
    ).scalars().all()
    now = utc_now()
    for row in rows:
        row.revoked_at = now
    session.flush()
    return len(rows)


def verify_account_email(session: Session, *, token: str, context: dict[str, str | None] | None = None) -> AppAccount | None:
    """Verify an account email from a one-time token."""
    token_row = consume_auth_token(session, token=token, token_type="email_verification")
    if token_row is None or token_row.account_id is None:
        return None
    account = session.get(AppAccount, token_row.account_id)
    if account is None:
        return None
    now = utc_now()
    account.email_verified_at = account.email_verified_at or now
    account.is_active = True
    account.updated_at = now
    record_auth_event(session, event_type="email_verified", account=account, context=context)
    return account


def request_password_reset(
    session: Session,
    *,
    email: str,
    context: dict[str, str | None] | None = None,
    settings: Settings | None = None,
) -> AuthEmailResult:
    """Create and send a password reset token when the account exists."""
    normalized_email = normalize_email(email)
    account = session.scalar(select(AppAccount).where(AppAccount.email == normalized_email))
    if account is None or not account.is_active:
        record_auth_event(session, event_type="password_reset_requested", email=normalized_email, context=context, details={"matched": False})
        return AuthEmailResult(sent=True)
    token = issue_auth_token(
        session,
        account=account,
        token_type="password_reset",
        duration=PASSWORD_RESET_DURATION,
        email=account.email,
    )
    record_auth_event(session, event_type="password_reset_requested", account=account, context=context, details={"matched": True})
    return send_password_reset_email(account=account, token=token, settings=settings)


def confirm_password_reset(
    session: Session,
    *,
    token: str,
    new_password: str,
    context: dict[str, str | None] | None = None,
) -> AppAccount | None:
    """Reset an account password from a valid one-time token."""
    token_row = consume_auth_token(session, token=token, token_type="password_reset")
    if token_row is None or token_row.account_id is None:
        return None
    account = session.get(AppAccount, token_row.account_id)
    if account is None or not account.is_active:
        return None
    now = utc_now()
    account.password_hash = hash_password(new_password)
    account.password_changed_at = now
    account.failed_login_count = 0
    account.locked_until = None
    account.updated_at = now
    revoked = revoke_account_sessions(session, account.account_id)
    record_auth_event(session, event_type="password_reset_confirmed", account=account, context=context, details={"revoked_sessions": revoked})
    return account


def generate_totp_secret() -> str:
    """Return a new base32 TOTP secret."""
    return base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")


def _totp_code(secret: str, for_time: int | None = None, step: int = 30, digits: int = 6) -> str:
    counter = int((for_time if for_time is not None else time.time()) // step)
    padded_secret = secret.upper() + "=" * ((8 - len(secret) % 8) % 8)
    key = base64.b32decode(padded_secret)
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(value % (10**digits)).zfill(digits)


def verify_totp(secret: str, code: str, *, window: int = 1) -> bool:
    """Verify a TOTP code with a small clock-skew window."""
    normalized = "".join(ch for ch in code if ch.isdigit())
    if len(normalized) != 6:
        return False
    now = int(time.time())
    return any(hmac.compare_digest(_totp_code(secret, now + (offset * 30)), normalized) for offset in range(-window, window + 1))


def start_mfa_setup(account: AppAccount, *, settings: Settings | None = None) -> dict[str, str]:
    """Return a new encrypted TOTP secret and otpauth URI payload."""
    secret = generate_totp_secret()
    issuer = "Orca Dashboard"
    label = f"{issuer}:{account.email}"
    account.mfa_secret_encrypted = encrypt_secret(secret, settings)
    account.mfa_enabled_at = None
    account.updated_at = utc_now()
    otpauth_uri = (
        "otpauth://totp/"
        f"{label}?secret={secret}&issuer={issuer.replace(' ', '%20')}&algorithm=SHA1&digits=6&period=30"
    )
    return {"secret": secret, "otpauth_uri": otpauth_uri}


def enable_mfa(
    session: Session,
    *,
    account: AppAccount,
    code: str,
    context: dict[str, str | None] | None = None,
    settings: Settings | None = None,
) -> bool:
    """Enable MFA after verifying the setup TOTP code."""
    if not account.mfa_secret_encrypted:
        return False
    secret = decrypt_secret(account.mfa_secret_encrypted, settings)
    if not verify_totp(secret, code):
        record_auth_event(session, event_type="mfa_enable_failed", account=account, context=context)
        return False
    account.mfa_enabled_at = utc_now()
    account.updated_at = utc_now()
    record_auth_event(session, event_type="mfa_enabled", account=account, context=context)
    return True


def issue_mfa_challenge(session: Session, *, account: AppAccount, context: dict[str, str | None] | None = None) -> str:
    """Create a short-lived MFA challenge token after password verification."""
    token = issue_auth_token(
        session,
        account=account,
        token_type="mfa_challenge",
        duration=MFA_CHALLENGE_DURATION,
        email=account.email,
    )
    record_auth_event(session, event_type="mfa_challenge_created", account=account, context=context)
    return token


def verify_mfa_challenge(
    session: Session,
    *,
    token: str,
    code: str,
    context: dict[str, str | None] | None = None,
    settings: Settings | None = None,
) -> AppAccount | None:
    """Verify a password-login MFA challenge."""
    token_row = consume_auth_token(session, token=token, token_type="mfa_challenge")
    if token_row is None or token_row.account_id is None:
        return None
    account = session.get(AppAccount, token_row.account_id)
    if account is None or not account.mfa_secret_encrypted or account.mfa_enabled_at is None:
        return None
    secret = decrypt_secret(account.mfa_secret_encrypted, settings)
    if not verify_totp(secret, code):
        record_auth_event(session, event_type="mfa_challenge_failed", account=account, context=context)
        return None
    record_auth_event(session, event_type="mfa_challenge_passed", account=account, context=context)
    return account


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
