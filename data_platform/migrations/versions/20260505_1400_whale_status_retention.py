"""finalize whale status for retention

Revision ID: 20260505_1400
Revises: 20260504_1200, 20260505_1000
Create Date: 2026-05-05 14:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260505_1400"
down_revision = ("20260504_1200", "20260505_1000")
branch_labels = None
depends_on = None


def _has_column(schema: str, table_name: str, column_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(column["name"] == column_name for column in inspector.get_columns(table_name, schema=schema))


def upgrade() -> None:
    if not _has_column("analytics", "user_profile", "whale_status"):
        op.add_column(
            "user_profile",
            sa.Column("whale_status", sa.Boolean(), nullable=True),
            schema="analytics",
        )

    op.execute(
        sa.text(
            """
            WITH latest_batch AS (
              SELECT snapshot_time, scoring_version
              FROM analytics.whale_score_snapshot
              ORDER BY snapshot_time DESC, whale_score_snapshot_id DESC
              LIMIT 1
            ),
            latest_scores AS (
              SELECT w.user_id, (w.is_whale OR w.is_trusted_whale) AS whale_status
              FROM analytics.whale_score_snapshot w
              JOIN latest_batch b
                ON b.snapshot_time = w.snapshot_time
               AND b.scoring_version = w.scoring_version
            )
            UPDATE analytics.user_profile up
               SET whale_status = COALESCE(ls.whale_status, FALSE)
              FROM latest_scores ls
             WHERE ls.user_id = up.user_id
            """
        )
    )
    op.execute(sa.text("UPDATE analytics.user_profile SET whale_status = FALSE WHERE whale_status IS NULL"))
    op.alter_column(
        "user_profile",
        "whale_status",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
        schema="analytics",
    )


def downgrade() -> None:
    if _has_column("analytics", "user_profile", "whale_status"):
        op.alter_column(
            "user_profile",
            "whale_status",
            existing_type=sa.Boolean(),
            nullable=True,
            server_default=None,
            schema="analytics",
        )
