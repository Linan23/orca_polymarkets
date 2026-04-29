"""add market-contract foreign-key indexes for scoped pruning

Revision ID: 20260425_1545
Revises: 20260425_1515
Create Date: 2026-04-25 15:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260425_1545"
down_revision = "20260425_1515"
branch_labels = None
depends_on = None


def _index_exists(schema: str, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name, schema=schema))


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str], *, schema: str) -> None:
    if _index_exists(schema, table_name, index_name):
        return
    op.create_index(index_name, table_name, columns, unique=False, schema=schema)


def _drop_index_if_exists(index_name: str, table_name: str, *, schema: str) -> None:
    if not _index_exists(schema, table_name, index_name):
        return
    op.drop_index(index_name, table_name=table_name, schema=schema)


def upgrade() -> None:
    _create_index_if_missing("ix_transaction_fact_market_contract_id", "transaction_fact", ["market_contract_id"], schema="analytics")
    _create_index_if_missing("ix_transaction_fact_part_market_contract_id", "transaction_fact_part", ["market_contract_id"], schema="analytics")
    _create_index_if_missing("ix_position_snapshot_market_contract_id", "position_snapshot", ["market_contract_id"], schema="analytics")
    _create_index_if_missing("ix_position_snapshot_part_market_contract_id", "position_snapshot_part", ["market_contract_id"], schema="analytics")
    _create_index_if_missing("ix_position_snapshot_daily_market_contract_id", "position_snapshot_daily", ["market_contract_id"], schema="analytics")


def downgrade() -> None:
    _drop_index_if_exists("ix_position_snapshot_daily_market_contract_id", "position_snapshot_daily", schema="analytics")
    _drop_index_if_exists("ix_position_snapshot_part_market_contract_id", "position_snapshot_part", schema="analytics")
    _drop_index_if_exists("ix_position_snapshot_market_contract_id", "position_snapshot", schema="analytics")
    _drop_index_if_exists("ix_transaction_fact_part_market_contract_id", "transaction_fact_part", schema="analytics")
    _drop_index_if_exists("ix_transaction_fact_market_contract_id", "transaction_fact", schema="analytics")
