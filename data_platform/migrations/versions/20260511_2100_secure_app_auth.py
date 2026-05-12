"""harden app account authentication

Revision ID: 20260511_2100
Revises: 20260506_1000
Create Date: 2026-05-11 21:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260511_2100"
down_revision = "20260506_1000"
branch_labels = None
depends_on = None

JSON_VARIANT = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def _column_exists(schema: str, table_name: str, column_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return column_name in {column["name"] for column in inspector.get_columns(table_name, schema=schema)}


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    """Add verification, reset, MFA, CSRF/session metadata, and auth audit tables."""
    if not _column_exists("app", "app_account", "email_verified_at"):
        op.add_column("app_account", sa.Column("email_verified_at", sa.DateTime(timezone=True), nullable=True), schema="app")
    if not _column_exists("app", "app_account", "mfa_secret_encrypted"):
        op.add_column("app_account", sa.Column("mfa_secret_encrypted", sa.Text(), nullable=True), schema="app")
    if not _column_exists("app", "app_account", "mfa_enabled_at"):
        op.add_column("app_account", sa.Column("mfa_enabled_at", sa.DateTime(timezone=True), nullable=True), schema="app")
    if not _column_exists("app", "app_account", "failed_login_count"):
        op.add_column(
            "app_account",
            sa.Column("failed_login_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            schema="app",
        )
        op.alter_column("app_account", "failed_login_count", server_default=None, schema="app")
    if not _column_exists("app", "app_account", "locked_until"):
        op.add_column("app_account", sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True), schema="app")
    if not _column_exists("app", "app_account", "password_changed_at"):
        op.add_column("app_account", sa.Column("password_changed_at", sa.DateTime(timezone=True), nullable=True), schema="app")
    op.execute(
        sa.text(
            """
            UPDATE app.app_account
            SET email_verified_at = COALESCE(email_verified_at, created_at)
            WHERE is_active = TRUE
              AND email_verified_at IS NULL
            """
        )
    )

    for column_name in ("csrf_token_hash", "user_agent_hash", "ip_prefix_hash"):
        if not _column_exists("app", "app_session", column_name):
            op.add_column("app_session", sa.Column(column_name, sa.String(length=128), nullable=True), schema="app")
    if not _column_exists("app", "app_session", "revoked_at"):
        op.add_column("app_session", sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True), schema="app")
    if not _column_exists("app", "app_session", "mfa_verified_at"):
        op.add_column("app_session", sa.Column("mfa_verified_at", sa.DateTime(timezone=True), nullable=True), schema="app")

    if not _table_exists("app", "app_auth_token"):
        op.create_table(
            "app_auth_token",
            sa.Column("token_id", sa.Integer(), primary_key=True),
            sa.Column("account_id", sa.Integer(), nullable=True),
            sa.Column("email", sa.String(length=320), nullable=True),
            sa.Column("token_hash", sa.String(length=128), nullable=False),
            sa.Column("token_type", sa.String(length=64), nullable=False),
            sa.Column("metadata_payload", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="CASCADE"),
            sa.UniqueConstraint("token_hash", name="uq_app_auth_token_hash"),
            schema="app",
        )
        op.create_index("ix_app_auth_token_account_type", "app_auth_token", ["account_id", "token_type"], schema="app")
        op.create_index("ix_app_auth_token_expires_at", "app_auth_token", ["expires_at"], schema="app")

    if not _table_exists("app", "app_auth_audit_log"):
        op.create_table(
            "app_auth_audit_log",
            sa.Column("auth_audit_log_id", sa.Integer(), primary_key=True),
            sa.Column("account_id", sa.Integer(), nullable=True),
            sa.Column("email", sa.String(length=320), nullable=True),
            sa.Column("event_type", sa.String(length=64), nullable=False),
            sa.Column("ip_prefix_hash", sa.String(length=128), nullable=True),
            sa.Column("user_agent_hash", sa.String(length=128), nullable=True),
            sa.Column("details_payload", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="SET NULL"),
            schema="app",
        )
        op.create_index("ix_app_auth_audit_log_account_time", "app_auth_audit_log", ["account_id", "created_at"], schema="app")
        op.create_index("ix_app_auth_audit_log_email_time", "app_auth_audit_log", ["email", "created_at"], schema="app")


def downgrade() -> None:
    """Remove hardened-auth additions."""
    if _table_exists("app", "app_auth_audit_log"):
        op.drop_index("ix_app_auth_audit_log_email_time", table_name="app_auth_audit_log", schema="app")
        op.drop_index("ix_app_auth_audit_log_account_time", table_name="app_auth_audit_log", schema="app")
        op.drop_table("app_auth_audit_log", schema="app")
    if _table_exists("app", "app_auth_token"):
        op.drop_index("ix_app_auth_token_expires_at", table_name="app_auth_token", schema="app")
        op.drop_index("ix_app_auth_token_account_type", table_name="app_auth_token", schema="app")
        op.drop_table("app_auth_token", schema="app")

    for column_name in ("mfa_verified_at", "revoked_at", "ip_prefix_hash", "user_agent_hash", "csrf_token_hash"):
        if _column_exists("app", "app_session", column_name):
            op.drop_column("app_session", column_name, schema="app")
    for column_name in (
        "password_changed_at",
        "locked_until",
        "failed_login_count",
        "mfa_enabled_at",
        "mfa_secret_encrypted",
        "email_verified_at",
    ):
        if _column_exists("app", "app_account", column_name):
            op.drop_column("app_account", column_name, schema="app")
