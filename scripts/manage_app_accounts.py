"""Manage production app accounts from the server terminal."""

from __future__ import annotations

import argparse
import getpass
import secrets
from pathlib import Path
import sys

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.db.session import session_scope
from data_platform.models import AppAccount
from data_platform.models.base import utc_now
from data_platform.services.account_auth import (
    create_account,
    normalize_account_role,
    normalize_display_name,
    normalize_email,
)


def _find_account(session: object, email: str) -> AppAccount | None:
    """Return one account by normalized email address."""
    normalized_email = normalize_email(email)
    return session.execute(select(AppAccount).where(AppAccount.email == normalized_email)).scalar_one_or_none()


def _print_account(account: AppAccount) -> None:
    """Print a compact summary row for one account."""
    print(
        f"{account.account_id}\t{account.email}\t{account.display_name}\t"
        f"{account.role}\t{'active' if account.is_active else 'inactive'}"
    )


def _resolve_password(args: argparse.Namespace) -> tuple[str, str | None]:
    """Resolve a password from CLI input, secure prompt, or generated secret."""
    if args.password:
        return args.password, None
    if args.generate_password:
        generated = secrets.token_urlsafe(18)
        return generated, generated
    prompted = getpass.getpass("Password: ")
    if not prompted:
        raise ValueError("Password is required.")
    return prompted, None


def cmd_list(args: argparse.Namespace) -> int:
    """List current app accounts."""
    with session_scope(args.database_url or None) as session:
        accounts = session.execute(select(AppAccount).order_by(AppAccount.created_at, AppAccount.account_id)).scalars().all()
        print("account_id\temail\tdisplay_name\trole\tstatus")
        for account in accounts:
            if not args.include_inactive and not account.is_active:
                continue
            _print_account(account)
    return 0


def cmd_create(args: argparse.Namespace) -> int:
    """Create a new account and optionally elevate its role."""
    password, generated_password = _resolve_password(args)
    with session_scope(args.database_url or None) as session:
        account = create_account(
            session,
            email=args.email,
            password=password,
            display_name=normalize_display_name(args.display_name),
        )
        account.role = normalize_account_role(args.role)
        account.updated_at = utc_now()
        print("Created account:")
        _print_account(account)
        if generated_password is not None:
            print(f"generated_password\t{generated_password}")
    return 0


def cmd_set_role(args: argparse.Namespace) -> int:
    """Update one account's role."""
    with session_scope(args.database_url or None) as session:
        account = _find_account(session, args.email)
        if account is None:
            raise LookupError(f"Account not found: {args.email}")
        account.role = normalize_account_role(args.role)
        account.updated_at = utc_now()
        print("Updated account:")
        _print_account(account)
    return 0


def cmd_set_active(args: argparse.Namespace, *, is_active: bool) -> int:
    """Toggle account activity."""
    with session_scope(args.database_url or None) as session:
        account = _find_account(session, args.email)
        if account is None:
            raise LookupError(f"Account not found: {args.email}")
        account.is_active = is_active
        account.updated_at = utc_now()
        print("Updated account:")
        _print_account(account)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(description="Manage production app accounts.")
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional SQLAlchemy database URL override. Defaults to DATABASE_URL from the environment.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List app accounts.")
    list_parser.add_argument("--include-inactive", action="store_true", help="Show inactive accounts too.")
    list_parser.set_defaults(func=cmd_list)

    create_parser = subparsers.add_parser("create", help="Create an app account.")
    create_parser.add_argument("--email", required=True, help="Account email.")
    create_parser.add_argument("--display-name", required=True, help="Display name shown in the app.")
    create_parser.add_argument("--password", default="", help="Initial password. Omit to prompt securely.")
    create_parser.add_argument(
        "--generate-password",
        action="store_true",
        help="Generate a random password and print it once to stdout.",
    )
    create_parser.add_argument(
        "--role",
        default="viewer",
        choices=["viewer", "moderator", "admin"],
        help="Initial account role.",
    )
    create_parser.set_defaults(func=cmd_create)

    role_parser = subparsers.add_parser("set-role", help="Change an account role.")
    role_parser.add_argument("--email", required=True, help="Existing account email.")
    role_parser.add_argument(
        "--role",
        required=True,
        choices=["viewer", "moderator", "admin"],
        help="New account role.",
    )
    role_parser.set_defaults(func=cmd_set_role)

    deactivate_parser = subparsers.add_parser("deactivate", help="Deactivate an account.")
    deactivate_parser.add_argument("--email", required=True, help="Existing account email.")
    deactivate_parser.set_defaults(func=lambda args: cmd_set_active(args, is_active=False))

    activate_parser = subparsers.add_parser("activate", help="Activate an account.")
    activate_parser.add_argument("--email", required=True, help="Existing account email.")
    activate_parser.set_defaults(func=lambda args: cmd_set_active(args, is_active=True))

    return parser


def main() -> int:
    """Run the account-management CLI."""
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
