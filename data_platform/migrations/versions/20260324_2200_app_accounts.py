"""add app account auth tables

Revision ID: 20260324_2200
Revises: 20260323_1200
Create Date: 2026-03-24 22:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260324_2200"
down_revision = "20260323_1200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the app auth, session, watchlist, and preference tables."""
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS app"))

    op.create_table(
        "app_account",
        sa.Column("account_id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=512), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("email", name="uq_app_account_email"),
        schema="app",
    )

    op.create_table(
        "app_session",
        sa.Column("session_id", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("session_token_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("session_token_hash", name="uq_app_session_token_hash"),
        schema="app",
    )
    op.create_index("ix_app_session_account_id", "app_session", ["account_id"], unique=False, schema="app")
    op.create_index("ix_app_session_expires_at", "app_session", ["expires_at"], unique=False, schema="app")

    op.create_table(
        "app_watchlist_user",
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("account_id", "user_id"),
        schema="app",
    )

    op.create_table(
        "app_watchlist_market",
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("market_slug", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("account_id", "market_slug"),
        schema="app",
    )

    op.create_table(
        "app_account_preferences",
        sa.Column("account_preferences_id", sa.Integer(), primary_key=True),
        sa.Column("account_id", sa.Integer(), nullable=False),
        sa.Column("preference_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["app.app_account.account_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("account_id", name="uq_app_account_preferences_account_id"),
        schema="app",
    )


def downgrade() -> None:
    """Drop the app auth, session, watchlist, and preference tables."""
    op.drop_table("app_account_preferences", schema="app")
    op.drop_table("app_watchlist_market", schema="app")
    op.drop_table("app_watchlist_user", schema="app")
    op.drop_index("ix_app_session_expires_at", table_name="app_session", schema="app")
    op.drop_index("ix_app_session_account_id", table_name="app_session", schema="app")
    op.drop_table("app_session", schema="app")
    op.drop_table("app_account", schema="app")
    op.execute(sa.text("DROP SCHEMA IF EXISTS app"))
