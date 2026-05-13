"""add whale score source data cutoff audit field

Revision ID: 20260428_1000
Revises: 20260427_1400
Create Date: 2026-04-28 10:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260428_1000"
down_revision = "20260427_1400"
branch_labels = None
depends_on = None


WHALE_SCORE_COLUMNS_WITH_CUTOFF = [
    "whale_score_snapshot_id",
    "user_id",
    "platform_id",
    "snapshot_time",
    "source_data_cutoff",
    "raw_volume_score",
    "consistency_score",
    "profitability_score",
    "trust_score",
    "insider_penalty",
    "is_whale",
    "is_trusted_whale",
    "sample_trade_count",
    "scoring_version",
    "created_at",
]

WHALE_SCORE_COLUMNS_WITHOUT_CUTOFF = [
    column for column in WHALE_SCORE_COLUMNS_WITH_CUTOFF if column != "source_data_cutoff"
]


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def _column_exists(schema: str, table_name: str, column_name: str) -> bool:
    if not _table_exists(schema, table_name):
        return False
    inspector = sa.inspect(op.get_bind())
    return column_name in {column["name"] for column in inspector.get_columns(table_name, schema=schema)}


def _index_exists(schema: str, table_name: str, index_name: str) -> bool:
    if not _table_exists(schema, table_name):
        return False
    inspector = sa.inspect(op.get_bind())
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name, schema=schema))


def _add_source_cutoff_column(table_name: str) -> None:
    if not _table_exists("analytics", table_name) or _column_exists("analytics", table_name, "source_data_cutoff"):
        return
    op.add_column(
        table_name,
        sa.Column("source_data_cutoff", sa.DateTime(timezone=True), nullable=True),
        schema="analytics",
    )


def _drop_source_cutoff_column(table_name: str) -> None:
    if _column_exists("analytics", table_name, "source_data_cutoff"):
        op.drop_column(table_name, "source_data_cutoff", schema="analytics")


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if _table_exists("analytics", table_name) and not _index_exists("analytics", table_name, index_name):
        op.create_index(index_name, table_name, columns, unique=False, schema="analytics")


def _drop_index_if_exists(index_name: str, table_name: str) -> None:
    if _index_exists("analytics", table_name, index_name):
        op.drop_index(index_name, table_name=table_name, schema="analytics")


def _create_whale_score_all_view(columns: list[str]) -> None:
    if not (_table_exists("analytics", "whale_score_snapshot") and _table_exists("analytics", "whale_score_snapshot_part")):
        return
    column_list = ", ".join(columns)
    op.execute(
        'DROP VIEW IF EXISTS analytics."whale_score_snapshot_all"'
    )
    op.execute(
        f'''
        CREATE VIEW analytics."whale_score_snapshot_all" AS
        SELECT {column_list}
        FROM analytics."whale_score_snapshot_part"
        UNION ALL
        SELECT {', '.join(f'l.{column}' for column in columns)}
        FROM analytics."whale_score_snapshot" AS l
        WHERE NOT EXISTS (
          SELECT 1
          FROM analytics."whale_score_snapshot_part" AS p
          WHERE p."whale_score_snapshot_id" = l."whale_score_snapshot_id"
        )
        '''
    )


def _backfill_legacy_source_cutoff() -> None:
    if not _column_exists("analytics", "whale_score_snapshot", "source_data_cutoff"):
        return
    op.execute(
        """
        WITH snapshot_platforms AS (
          SELECT DISTINCT platform_id, snapshot_time
          FROM analytics.whale_score_snapshot
          WHERE source_data_cutoff IS NULL
        ),
        cutoffs AS (
          SELECT
            sp.platform_id,
            sp.snapshot_time,
            MAX(tf.transaction_time) AS source_data_cutoff
          FROM snapshot_platforms sp
          LEFT JOIN analytics.transaction_fact tf
            ON tf.platform_id = sp.platform_id
           AND tf.transaction_time <= sp.snapshot_time
          GROUP BY sp.platform_id, sp.snapshot_time
        )
        UPDATE analytics.whale_score_snapshot w
        SET source_data_cutoff = COALESCE(c.source_data_cutoff, w.snapshot_time)
        FROM cutoffs c
        WHERE w.platform_id = c.platform_id
          AND w.snapshot_time = c.snapshot_time
          AND w.source_data_cutoff IS NULL
        """
    )


def _backfill_part_source_cutoff() -> None:
    if not _column_exists("analytics", "whale_score_snapshot_part", "source_data_cutoff"):
        return
    if _column_exists("analytics", "whale_score_snapshot", "source_data_cutoff"):
        op.execute(
            """
            UPDATE analytics.whale_score_snapshot_part p
            SET source_data_cutoff = w.source_data_cutoff
            FROM analytics.whale_score_snapshot w
            WHERE p.whale_score_snapshot_id = w.whale_score_snapshot_id
              AND p.source_data_cutoff IS NULL
              AND w.source_data_cutoff IS NOT NULL
            """
        )
    op.execute(
        """
        WITH snapshot_platforms AS (
          SELECT DISTINCT platform_id, snapshot_time
          FROM analytics.whale_score_snapshot_part
          WHERE source_data_cutoff IS NULL
        ),
        cutoffs AS (
          SELECT
            sp.platform_id,
            sp.snapshot_time,
            MAX(tf.transaction_time) AS source_data_cutoff
          FROM snapshot_platforms sp
          LEFT JOIN analytics.transaction_fact tf
            ON tf.platform_id = sp.platform_id
           AND tf.transaction_time <= sp.snapshot_time
          GROUP BY sp.platform_id, sp.snapshot_time
        )
        UPDATE analytics.whale_score_snapshot_part p
        SET source_data_cutoff = COALESCE(c.source_data_cutoff, p.snapshot_time)
        FROM cutoffs c
        WHERE p.platform_id = c.platform_id
          AND p.snapshot_time = c.snapshot_time
          AND p.source_data_cutoff IS NULL
        """
    )


def upgrade() -> None:
    _add_source_cutoff_column("whale_score_snapshot")
    _add_source_cutoff_column("whale_score_snapshot_part")
    # Do not run historical cutoff backfills or large index builds during
    # service startup. On a populated VM these can hold migration locks long
    # enough to keep the API offline. New whale score jobs write this field
    # directly; older rows and optional indexes should be handled by deliberate
    # maintenance jobs.
    _create_whale_score_all_view(WHALE_SCORE_COLUMNS_WITH_CUTOFF)


def downgrade() -> None:
    _drop_index_if_exists("ix_whale_score_snapshot_part_source_cutoff", "whale_score_snapshot_part")
    _drop_index_if_exists("ix_whale_score_snapshot_source_cutoff", "whale_score_snapshot")
    _create_whale_score_all_view(WHALE_SCORE_COLUMNS_WITHOUT_CUTOFF)
    _drop_source_cutoff_column("whale_score_snapshot_part")
    _drop_source_cutoff_column("whale_score_snapshot")
