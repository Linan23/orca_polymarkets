"""add preferred username to user accounts

Revision ID: 20260323_1200
Revises: 20260304_1200
Create Date: 2026-03-23 12:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260323_1200"
down_revision = "20260304_1200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add a canonical preferred username field and backfill it from raw Polymarket trades."""
    op.add_column("user_account", sa.Column("preferred_username", sa.String(length=255), nullable=True), schema="analytics")
    op.execute(
        sa.text(
            """
            WITH polymarket_users AS (
              SELECT
                ua.user_id,
                lower(COALESCE(ua.wallet_address, ua.external_user_ref, '')) AS wallet
              FROM analytics.user_account ua
              JOIN analytics.platform p
                ON p.platform_id = ua.platform_id
              WHERE p.platform_name = 'polymarket'
                AND COALESCE(ua.preferred_username, '') = ''
                AND COALESCE(ua.wallet_address, ua.external_user_ref, '') <> ''
            ),
            latest_names AS (
              SELECT DISTINCT ON (pu.user_id)
                pu.user_id,
                NULLIF(BTRIM(trade->>'name'), '') AS preferred_username
              FROM raw.api_payload payload_row
              CROSS JOIN LATERAL jsonb_array_elements(payload_row.payload->'trades') AS trade
              JOIN polymarket_users pu
                ON pu.wallet = lower(COALESCE(trade->>'proxyWallet', ''))
              WHERE payload_row.entity_type = 'trades'
                AND COALESCE(trade->>'proxyWallet', '') <> ''
                AND COALESCE(trade->>'name', '') <> ''
                AND NOT (trade->>'name' ~* '^0x[0-9a-f]{8,}(-[0-9]+)?$')
              ORDER BY pu.user_id, (trade->>'timestamp')::bigint DESC
            )
            UPDATE analytics.user_account ua
            SET preferred_username = latest_names.preferred_username
            FROM latest_names
            WHERE ua.user_id = latest_names.user_id
            """
        )
    )


def downgrade() -> None:
    """Drop the canonical preferred username field."""
    op.drop_column("user_account", "preferred_username", schema="analytics")
