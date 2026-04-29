"""add persisted resolved condition table

Revision ID: 20260427_1200
Revises: 20260425_1545
Create Date: 2026-04-27 12:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260427_1200"
down_revision = "20260425_1545"
branch_labels = None
depends_on = None


MONEY = sa.Numeric(20, 8)


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def _index_exists(schema: str, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name, schema=schema))


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str], *, schema: str) -> None:
    if _index_exists(schema, table_name, index_name):
        return
    op.create_index(index_name, table_name, columns, unique=False, schema=schema)


def _drop_index_if_exists(index_name: str, table_name: str, *, schema: str) -> None:
    if not _table_exists(schema, table_name) or not _index_exists(schema, table_name, index_name):
        return
    op.drop_index(index_name, table_name=table_name, schema=schema)


def upgrade() -> None:
    if not _table_exists("analytics", "resolved_condition"):
        op.create_table(
            "resolved_condition",
            sa.Column("resolved_condition_id", sa.Integer(), primary_key=True),
            sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
            sa.Column("condition_ref", sa.String(length=255), nullable=False),
            sa.Column("resolver_method", sa.String(length=64), nullable=False),
            sa.Column("winning_outcome_label", sa.String(length=128), nullable=False),
            sa.Column("resolved_at", sa.DateTime(timezone=True)),
            sa.Column("max_winning_price", MONEY),
            sa.Column("min_losing_price", MONEY),
            sa.Column("trade_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("confidence", sa.Numeric(6, 4), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.UniqueConstraint("platform_id", "condition_ref", name="uq_resolved_condition_platform_condition"),
            schema="analytics",
        )
    _create_index_if_missing("ix_resolved_condition_method", "resolved_condition", ["resolver_method"], schema="analytics")
    _create_index_if_missing("ix_resolved_condition_resolved_at", "resolved_condition", ["resolved_at"], schema="analytics")


def downgrade() -> None:
    _drop_index_if_exists("ix_resolved_condition_resolved_at", "resolved_condition", schema="analytics")
    _drop_index_if_exists("ix_resolved_condition_method", "resolved_condition", schema="analytics")
    if _table_exists("analytics", "resolved_condition"):
        op.drop_table("resolved_condition", schema="analytics")
