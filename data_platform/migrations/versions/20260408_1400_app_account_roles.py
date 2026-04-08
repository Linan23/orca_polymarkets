"""add app-account roles

Revision ID: 20260408_1400
Revises: 20260325_1100
Create Date: 2026-04-08 14:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260408_1400"
down_revision = "20260325_1100"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add role-based authorization to app accounts."""
    op.add_column(
        "app_account",
        sa.Column("role", sa.String(length=32), nullable=False, server_default=sa.text("'viewer'")),
        schema="app",
    )
    op.create_check_constraint(
        "ck_app_account_role",
        "app_account",
        "role IN ('viewer', 'moderator', 'admin')",
        schema="app",
    )
    op.alter_column("app_account", "role", server_default=None, schema="app")


def downgrade() -> None:
    """Remove role-based authorization from app accounts."""
    op.drop_constraint("ck_app_account_role", "app_account", schema="app")
    op.drop_column("app_account", "role", schema="app")

