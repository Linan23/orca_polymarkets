"""Persist inferred resolved Polymarket conditions for reusable whale analytics."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


RESOLUTION_PRICE_HIGH = 0.98
RESOLUTION_PRICE_LOW = 0.02

_REFRESH_RESOLVED_CONDITIONS_SQL = text(
    """
    WITH polymarket AS (
      SELECT platform_id
      FROM analytics.platform
      WHERE platform_name = 'polymarket'
      LIMIT 1
    ),
    closed_markets AS (
      SELECT
        mc.market_contract_id,
        mc.platform_id,
        mc.condition_ref,
        LOWER(TRIM(mc.outcome_a_label)) AS outcome_a_label,
        LOWER(TRIM(mc.outcome_b_label)) AS outcome_b_label,
        mc.last_trade_price,
        COALESCE(mc.end_time, me.end_time, me.closed_time, mc.updated_at) AS resolved_at
      FROM analytics.market_contract mc
      JOIN analytics.market_event me
        ON me.event_id = mc.event_id
      JOIN polymarket p
        ON p.platform_id = mc.platform_id
      WHERE mc.is_closed = TRUE
        AND mc.condition_ref IS NOT NULL
        AND mc.outcome_a_label IS NOT NULL
        AND mc.outcome_b_label IS NOT NULL
        AND LOWER(TRIM(mc.outcome_a_label)) <> LOWER(TRIM(mc.outcome_b_label))
    ),
    closed_market_trade_stats AS (
      SELECT
        cm.platform_id,
        cm.condition_ref,
        LOWER(TRIM(tf.outcome_label)) AS outcome_label,
        MAX(tf.price) AS max_trade_price,
        MIN(tf.price) AS min_trade_price,
        COUNT(*) AS trade_count,
        MAX(tf.transaction_time) AS latest_trade_time
      FROM closed_markets cm
      JOIN analytics.transaction_fact tf
        ON tf.market_contract_id = cm.market_contract_id
       AND tf.platform_id = cm.platform_id
      WHERE tf.outcome_label IS NOT NULL
        AND tf.price IS NOT NULL
      GROUP BY cm.platform_id, cm.condition_ref, LOWER(TRIM(tf.outcome_label))
    ),
    closed_market_trade_counts AS (
      SELECT
        platform_id,
        condition_ref,
        SUM(trade_count)::integer AS trade_count,
        MAX(latest_trade_time) AS latest_trade_time
      FROM closed_market_trade_stats
      GROUP BY platform_id, condition_ref
    ),
    broad_trade_stats AS (
      SELECT
        tf.platform_id,
        mc.condition_ref,
        LOWER(TRIM(tf.outcome_label)) AS outcome_label,
        MAX(tf.price) AS max_trade_price,
        MIN(tf.price) AS min_trade_price,
        COUNT(*) AS trade_count,
        MAX(tf.transaction_time) AS latest_trade_time
      FROM analytics.transaction_fact tf
      JOIN analytics.market_contract mc
        ON mc.market_contract_id = tf.market_contract_id
      JOIN polymarket p
        ON p.platform_id = tf.platform_id
      WHERE mc.condition_ref IS NOT NULL
        AND tf.outcome_label IS NOT NULL
        AND tf.price IS NOT NULL
      GROUP BY tf.platform_id, mc.condition_ref, LOWER(TRIM(tf.outcome_label))
    ),
    broad_binary_conditions AS (
      SELECT
        platform_id,
        condition_ref,
        SUM(trade_count)::integer AS trade_count,
        MAX(latest_trade_time) AS latest_trade_time
      FROM broad_trade_stats
      GROUP BY platform_id, condition_ref
      HAVING COUNT(DISTINCT outcome_label) = 2
    ),
    last_trade_candidates AS (
      SELECT
        cm.platform_id,
        cm.condition_ref,
        'last_trade_price_threshold'::text AS resolver_method,
        CASE
          WHEN cm.last_trade_price >= :price_high THEN cm.outcome_a_label
          WHEN cm.last_trade_price <= :price_low THEN cm.outcome_b_label
        END AS winning_outcome_label,
        cm.resolved_at,
        cm.last_trade_price AS max_winning_price,
        NULL::numeric AS min_losing_price,
        COALESCE(ctc.trade_count, 0) AS trade_count,
        0.7500::numeric AS confidence,
        1 AS priority
      FROM closed_markets cm
      LEFT JOIN closed_market_trade_counts ctc
        ON ctc.platform_id = cm.platform_id
       AND ctc.condition_ref = cm.condition_ref
      WHERE cm.last_trade_price >= :price_high
         OR cm.last_trade_price <= :price_low
    ),
    trade_pair_candidates AS (
      SELECT
        ctc.platform_id,
        ctc.condition_ref,
        'trade_price_extreme_fallback'::text AS resolver_method,
        winner.outcome_label AS winning_outcome_label,
        ctc.latest_trade_time AS resolved_at,
        winner.max_trade_price AS max_winning_price,
        loser.min_trade_price AS min_losing_price,
        COALESCE(ctc.trade_count, 0) AS trade_count,
        0.6000::numeric AS confidence,
        2 AS priority,
        ROW_NUMBER() OVER (
          PARTITION BY ctc.platform_id, ctc.condition_ref
          ORDER BY winner.max_trade_price DESC, loser.min_trade_price ASC, COALESCE(ctc.trade_count, 0) DESC
        ) AS rn
      FROM broad_binary_conditions ctc
      JOIN broad_trade_stats winner
        ON winner.platform_id = ctc.platform_id
       AND winner.condition_ref = ctc.condition_ref
      JOIN broad_trade_stats loser
        ON loser.platform_id = ctc.platform_id
       AND loser.condition_ref = ctc.condition_ref
       AND loser.outcome_label <> winner.outcome_label
      WHERE winner.max_trade_price >= :price_high
        AND loser.min_trade_price <= :price_low
    ),
    candidates AS (
      SELECT * FROM last_trade_candidates WHERE winning_outcome_label IS NOT NULL
      UNION ALL
      SELECT
        platform_id,
        condition_ref,
        resolver_method,
        winning_outcome_label,
        resolved_at,
        max_winning_price,
        min_losing_price,
        trade_count,
        confidence,
        priority
      FROM trade_pair_candidates
      WHERE rn = 1
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY platform_id, condition_ref
          ORDER BY priority ASC, confidence DESC, trade_count DESC
        ) AS rn
      FROM candidates
    ),
    upserted AS (
      INSERT INTO analytics.resolved_condition (
        platform_id,
        condition_ref,
        resolver_method,
        winning_outcome_label,
        resolved_at,
        max_winning_price,
        min_losing_price,
        trade_count,
        confidence,
        created_at,
        updated_at
      )
      SELECT
        platform_id,
        condition_ref,
        resolver_method,
        winning_outcome_label,
        resolved_at,
        max_winning_price,
        min_losing_price,
        trade_count,
        confidence,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
      FROM ranked
      WHERE rn = 1
      ON CONFLICT (platform_id, condition_ref) DO UPDATE SET
        resolver_method = EXCLUDED.resolver_method,
        winning_outcome_label = EXCLUDED.winning_outcome_label,
        resolved_at = EXCLUDED.resolved_at,
        max_winning_price = EXCLUDED.max_winning_price,
        min_losing_price = EXCLUDED.min_losing_price,
        trade_count = EXCLUDED.trade_count,
        confidence = EXCLUDED.confidence,
        updated_at = CURRENT_TIMESTAMP
      RETURNING resolver_method
    )
    SELECT
      COUNT(*)::integer AS rows_written,
      COUNT(*) FILTER (WHERE resolver_method = 'last_trade_price_threshold')::integer AS last_trade_rows,
      COUNT(*) FILTER (WHERE resolver_method = 'trade_price_extreme_fallback')::integer AS trade_stream_rows
    FROM upserted
    """
)

_RESOLVED_CONDITION_SUMMARY_SQL = text(
    """
    SELECT
      COUNT(*)::integer AS resolved_conditions,
      COUNT(*) FILTER (WHERE resolver_method = 'last_trade_price_threshold')::integer AS last_trade_conditions,
      COUNT(*) FILTER (WHERE resolver_method = 'trade_price_extreme_fallback')::integer AS trade_stream_conditions,
      COALESCE(SUM(trade_count), 0)::integer AS source_trade_count,
      MAX(updated_at) AS latest_updated_at
    FROM analytics.resolved_condition rc
    JOIN analytics.platform p
      ON p.platform_id = rc.platform_id
    WHERE p.platform_name = 'polymarket'
    """
)

_RESOLVED_CONDITION_COVERAGE_SQL = text(
    """
    SELECT
      COUNT(tf.transaction_id)::integer AS resolved_trade_rows,
      COUNT(DISTINCT tf.user_id)::integer AS resolved_trade_users,
      COUNT(DISTINCT tf.market_contract_id)::integer AS resolved_trade_contracts,
      COUNT(DISTINCT mc.condition_ref)::integer AS resolved_trade_conditions,
      COALESCE(SUM(tf.notional_value), 0) AS resolved_trade_notional
    FROM analytics.transaction_fact tf
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.resolved_condition rc
      ON rc.platform_id = tf.platform_id
     AND rc.condition_ref = mc.condition_ref
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = 'polymarket'
    """
)


def refresh_resolved_conditions(session: Session) -> dict[str, Any]:
    """Upsert confidently inferred Polymarket condition outcomes and return coverage."""
    written = session.execute(
        _REFRESH_RESOLVED_CONDITIONS_SQL,
        {"price_high": RESOLUTION_PRICE_HIGH, "price_low": RESOLUTION_PRICE_LOW},
    ).mappings().one()
    summary = session.execute(_RESOLVED_CONDITION_SUMMARY_SQL).mappings().one()
    coverage = session.execute(_RESOLVED_CONDITION_COVERAGE_SQL).mappings().one()
    return {
        "rows_written": int(written["rows_written"] or 0),
        "last_trade_rows_written": int(written["last_trade_rows"] or 0),
        "trade_stream_rows_written": int(written["trade_stream_rows"] or 0),
        "resolved_conditions": int(summary["resolved_conditions"] or 0),
        "last_trade_conditions": int(summary["last_trade_conditions"] or 0),
        "trade_stream_conditions": int(summary["trade_stream_conditions"] or 0),
        "source_trade_count": int(summary["source_trade_count"] or 0),
        "latest_updated_at": summary["latest_updated_at"].isoformat() if summary["latest_updated_at"] else None,
        "resolved_trade_rows": int(coverage["resolved_trade_rows"] or 0),
        "resolved_trade_users": int(coverage["resolved_trade_users"] or 0),
        "resolved_trade_contracts": int(coverage["resolved_trade_contracts"] or 0),
        "resolved_trade_conditions": int(coverage["resolved_trade_conditions"] or 0),
        "resolved_trade_notional": float(coverage["resolved_trade_notional"] or 0),
    }
