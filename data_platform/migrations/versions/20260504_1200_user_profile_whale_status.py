"""add passive whale status field to user profiles

Revision ID: 20260504_1200
Revises: 20260429_1800
Create Date: 2026-05-04 12:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260504_1200"
down_revision = "20260429_1800"
branch_labels = None
depends_on = None


def _has_column(schema: str, table_name: str, column_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(column["name"] == column_name for column in inspector.get_columns(table_name, schema=schema))


def upgrade() -> None:
    if not _has_column("analytics", "user_profile", "whale_status"):
        op.add_column("user_profile", sa.Column("whale_status", sa.Boolean(), nullable=True), schema="analytics")


def downgrade() -> None:
    if _has_column("analytics", "user_profile", "whale_status"):
        op.drop_column("user_profile", "whale_status", schema="analytics")
