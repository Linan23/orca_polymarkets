"""Focused auth hardening checks for signup, CSRF, reset, and MFA flows."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

os.environ["APP_ENV"] = "development"
os.environ["ALLOWED_SIGNUP_EMAIL_DOMAINS"] = "example.com"
os.environ["SMTP_HOST"] = ""

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient
from sqlalchemy import text

from data_platform.api.server import app
from data_platform.db.session import session_scope
from data_platform.services.account_auth import _totp_code


def _assert(name: str, ok: bool, details: dict[str, Any] | None = None) -> None:
    status = "ok" if ok else "fail"
    print(f"{status}\t{name}\t{details or {}}")
    if not ok:
        raise SystemExit(1)


def _csrf(payload: dict[str, Any]) -> dict[str, str]:
    token = payload.get("csrf_token")
    return {"X-CSRF-Token": str(token)} if token else {}


def _json(response: Any) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_body": response.text[:200]}
    return payload if isinstance(payload, dict) else {"payload": payload}


def main() -> int:
    email = f"auth-check-{uuid4().hex[:12]}@example.com"
    bad_domain_email = f"auth-check-{uuid4().hex[:12]}@blocked.test"
    password = "AuthCheckPass123!"
    reset_password = "AuthCheckReset123!"

    with TestClient(app) as client:
        rejected = client.post(
            "/api/auth/signup",
            json={"display_name": "Blocked User", "email": bad_domain_email, "password": password},
        )
        _assert("signup rejects unapproved domain", rejected.status_code == 403, {"status_code": rejected.status_code})

        signup = client.post(
            "/api/auth/signup",
            json={"display_name": "Auth Check", "email": email, "password": password},
        )
        signup_payload = _json(signup)
        _assert("signup creates unverified account", signup.status_code == 200 and bool(signup_payload.get("verification_required")), signup_payload)

        blocked_login = client.post("/api/auth/login", json={"email": email, "password": password})
        _assert("unverified login is blocked", blocked_login.status_code == 401, {"status_code": blocked_login.status_code})

        verify_token = signup_payload.get("dev_verification_token")
        _assert("dev verification token available", isinstance(verify_token, str) and bool(verify_token), signup_payload)
        verify = client.post("/api/auth/verify-email", json={"token": verify_token})
        verify_payload = _json(verify)
        _assert("verification signs in", verify.status_code == 200 and "session" in verify_payload, verify_payload)

        missing_csrf = client.patch("/api/account/preferences", json={"homepage": {"research_timeframe": "30d"}})
        _assert("mutating route requires CSRF", missing_csrf.status_code == 403, {"status_code": missing_csrf.status_code})

        csrf_headers = _csrf(verify_payload)
        valid_csrf = client.patch(
            "/api/account/preferences",
            headers=csrf_headers,
            json={"homepage": {"research_timeframe": "30d"}},
        )
        _assert("mutating route accepts CSRF", valid_csrf.status_code == 200, {"status_code": valid_csrf.status_code})

        reset_request = client.post("/api/auth/password-reset/request", json={"email": email})
        reset_payload = _json(reset_request)
        reset_token = reset_payload.get("dev_reset_token")
        _assert("password reset creates token", reset_request.status_code == 200 and isinstance(reset_token, str), reset_payload)

        reset_confirm = client.post(
            "/api/auth/password-reset/confirm",
            json={"token": reset_token, "password": reset_password},
        )
        reset_confirm_payload = _json(reset_confirm)
        _assert("password reset signs in fresh session", reset_confirm.status_code == 200 and "session" in reset_confirm_payload, reset_confirm_payload)

        with session_scope() as session:
            session.execute(
                text("UPDATE app.app_account SET role = 'moderator', updated_at = now() WHERE email = :email"),
                {"email": email},
            )

        setup = client.post("/api/auth/mfa/setup", headers=_csrf(reset_confirm_payload))
        setup_payload = _json(setup)
        secret = setup_payload.get("secret")
        _assert("moderator can start MFA setup", setup.status_code == 200 and isinstance(secret, str), setup_payload)

        enable = client.post("/api/auth/mfa/enable", headers=_csrf(reset_confirm_payload), json={"code": _totp_code(str(secret))})
        enable_payload = _json(enable)
        _assert("moderator can enable MFA", enable.status_code == 200 and "session" in enable_payload, enable_payload)

        logout = client.post("/api/auth/logout", headers=_csrf(enable_payload))
        _assert("logout with CSRF succeeds", logout.status_code == 200, {"status_code": logout.status_code})

        mfa_login = client.post("/api/auth/login", json={"email": email, "password": reset_password})
        mfa_login_payload = _json(mfa_login)
        _assert("moderator password login requires MFA", mfa_login.status_code == 200 and bool(mfa_login_payload.get("mfa_required")), mfa_login_payload)

        mfa_token = mfa_login_payload.get("mfa_token")
        mfa_verify = client.post("/api/auth/mfa/verify", json={"mfa_token": mfa_token, "code": _totp_code(str(secret))})
        mfa_verify_payload = _json(mfa_verify)
        _assert("MFA challenge creates session", mfa_verify.status_code == 200 and "session" in mfa_verify_payload, mfa_verify_payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
