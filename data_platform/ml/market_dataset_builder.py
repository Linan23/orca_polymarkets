"""Build the first market-level ML dataset from resolved Polymarket conditions."""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_platform.ingest.store import UNKNOWN_USER_EXTERNAL_REF
from data_platform.services.whale_scoring import (
    ResolvedUserPerformance,
    WhaleMetricInput,
    compute_whale_scores,
    load_resolved_market_outcome_details,
)
from data_platform.ml.whale_weighting import (
    WhaleWeightConfig,
    compute_weighted_whale_score,
    load_whale_weight_config,
)


DATASET_VERSION = "ml_market_snapshot_v9"
DEFAULT_HORIZON_HOURS = (720, 168, 72, 24, 6, 1)
FUTURE_MOVEMENT_WINDOWS_HOURS = (12, 24)
RECENT_WHALE_WINDOWS_HOURS = (1, 6, 12, 24)
DEFAULT_OUTPUT_DIR = Path("data_platform/runtime/ml")
DEFAULT_DATASET_PATH = DEFAULT_OUTPUT_DIR / "resolved_market_snapshot_features.csv"
DEFAULT_METADATA_PATH = DEFAULT_OUTPUT_DIR / "resolved_market_snapshot_features.metadata.json"
TARGET_COLUMN = "label_side_wins"
GROUP_KEY_COLUMN = "condition_ref"
PRICE_BASELINE_COLUMN = "price_baseline"
RESOLUTION_EDGE_COLUMN = "resolution_edge"
RECENT_TRUSTED_WHALE_FEATURE_COLUMNS = tuple(
    f"trusted_whale_side_recent_{metric}_{window}h"
    for window in RECENT_WHALE_WINDOWS_HOURS
    for metric in (
        "trade_count",
        "distinct_users",
        "entry_trade_count",
        "exit_trade_count",
        "entry_notional",
        "exit_notional",
        "net_notional",
        "net_notional_share",
        "weighted_entry_pressure",
        "weighted_exit_pressure",
        "weighted_net_pressure",
        "decay_weighted_entry_pressure",
        "decay_weighted_exit_pressure",
        "decay_weighted_net_pressure",
        "entry_exit_ratio",
    )
)
NORMALIZED_TRUSTED_WHALE_PRESSURE_FEATURE_COLUMNS = (
    "trusted_whale_weighted_net_pressure_total_per_total_notional",
    "trusted_whale_weighted_net_pressure_total_per_market_liquidity",
    "trusted_whale_weighted_net_pressure_total_per_trusted_whale",
    "trusted_whale_side_weighted_net_pressure_per_side_notional",
    "trusted_whale_side_weighted_net_pressure_per_total_notional",
    "trusted_whale_side_weighted_net_pressure_per_market_liquidity",
    "trusted_whale_side_weighted_net_pressure_per_trusted_whale",
)
SCORED_WHALE_TOTAL_PRESSURE_FEATURE_COLUMNS = (
    "whale_weighted_score_sum_total",
    "whale_weighted_net_pressure_total",
    "whale_weighted_net_pressure_total_per_total_notional",
    "whale_weighted_net_pressure_total_per_market_liquidity",
    "whale_weighted_net_pressure_total_per_whale",
)
SCORED_WHALE_SIDE_PRESSURE_FEATURE_COLUMNS = (
    "whale_side_weighted_score_sum",
    "whale_side_weighted_buy_pressure",
    "whale_side_weighted_sell_pressure",
    "whale_side_weighted_net_pressure",
    "whale_side_weighted_net_pressure_per_side_notional",
    "whale_side_weighted_net_pressure_per_total_notional",
    "whale_side_weighted_net_pressure_per_market_liquidity",
    "whale_side_weighted_net_pressure_per_whale",
)
SCORED_WHALE_PRESSURE_FEATURE_COLUMNS = (
    *SCORED_WHALE_TOTAL_PRESSURE_FEATURE_COLUMNS,
    *SCORED_WHALE_SIDE_PRESSURE_FEATURE_COLUMNS,
)

FEATURE_COLUMNS = (
    "horizon_hours",
    "hours_to_close",
    "market_age_hours",
    "market_duration_hours",
    "side_position_feature",
    "question_char_length",
    "question_token_count",
    "event_title_char_length",
    "event_title_token_count",
    "event_description_char_length",
    "event_description_token_count",
    "event_category_token_count",
    "event_tag_count",
    "event_tag_token_count",
    "market_slug_token_count",
    "event_slug_token_count",
    "question_title_token_overlap",
    "question_description_token_overlap",
    "question_category_token_overlap",
    "question_tag_token_overlap",
    "question_side_token_overlap",
    "question_opposite_token_overlap",
    "side_category_token_overlap",
    "opposite_side_category_token_overlap",
    "side_tag_token_overlap",
    "opposite_side_tag_token_overlap",
    "side_label_char_length",
    "side_label_token_count",
    "opposite_side_label_char_length",
    "opposite_side_label_token_count",
    "side_label_is_yes",
    "side_label_is_no",
    "opposite_label_is_yes",
    "opposite_label_is_no",
    "question_has_number",
    "question_digit_count",
    "question_has_percent",
    "question_has_dollar",
    "question_has_more_than",
    "question_has_less_than",
    "question_has_date_reference",
    "question_starts_with_who_or_which",
    "question_title_exact_match",
    "market_volume_log1p",
    "market_liquidity_log1p",
    "prior_side_label_count",
    "prior_side_label_win_rate",
    "prior_label_pair_count",
    "prior_label_pair_side_win_rate",
    "prior_question_pattern_count",
    "prior_question_pattern_side_win_rate",
    "prior_side_position_count",
    "prior_side_position_win_rate",
    "prior_event_category_count",
    "prior_event_category_side_win_rate",
    "prior_question_opening_count",
    "prior_question_opening_side_win_rate",
    "prior_category_pattern_count",
    "prior_category_pattern_side_win_rate",
    "has_any_trade_before_cutoff",
    "has_both_outcomes_before_cutoff",
    "side_has_trade_before_cutoff",
    "opposite_has_trade_before_cutoff",
    "side_price_observed",
    "opposite_price_observed",
    "trade_count_total",
    "trade_count_outcome_a",
    "trade_count_outcome_b",
    "buy_trade_count_total",
    "sell_trade_count_total",
    "distinct_users",
    "top_user_trade_share",
    "top_user_notional_share",
    "total_notional",
    "total_buy_notional",
    "total_sell_notional",
    "whale_distinct_users",
    "trusted_whale_distinct_users",
    "whale_trade_share",
    "trusted_whale_trade_share",
    "whale_notional_share",
    "trusted_whale_notional_share",
    "whale_buy_trade_count_total",
    "whale_sell_trade_count_total",
    *SCORED_WHALE_TOTAL_PRESSURE_FEATURE_COLUMNS,
    "trusted_whale_buy_trade_count_total",
    "trusted_whale_sell_trade_count_total",
    "trusted_whale_buy_sell_ratio_total",
    "trusted_whale_weighted_score_sum_total",
    "trusted_whale_weighted_net_pressure_total",
    "trusted_whale_weighted_net_pressure_total_per_total_notional",
    "trusted_whale_weighted_net_pressure_total_per_market_liquidity",
    "trusted_whale_weighted_net_pressure_total_per_trusted_whale",
    "side_trade_count",
    "opposite_trade_count",
    "side_distinct_users",
    "opposite_distinct_users",
    "side_buy_notional",
    "side_sell_notional",
    "opposite_buy_notional",
    "opposite_sell_notional",
    "side_net_notional",
    "opposite_net_notional",
    "side_total_shares",
    "opposite_total_shares",
    "last_price_side",
    "last_price_opposite",
    "avg_price_side",
    "avg_price_opposite",
    "min_price_side",
    "min_price_opposite",
    "max_price_side",
    "max_price_opposite",
    "side_trade_share",
    "side_buy_notional_share",
    "whale_side_trade_share",
    "trusted_whale_side_trade_share",
    "whale_side_notional_share",
    "trusted_whale_side_notional_share",
    "whale_side_buy_notional_share",
    "whale_side_sell_notional_share",
    "trusted_whale_side_buy_notional_share",
    "trusted_whale_side_sell_notional_share",
    "whale_side_net_notional_share",
    *SCORED_WHALE_SIDE_PRESSURE_FEATURE_COLUMNS,
    "trusted_whale_side_net_notional_share",
    "trusted_whale_side_buy_trade_count",
    "trusted_whale_side_sell_trade_count",
    "trusted_whale_side_buy_sell_ratio",
    "trusted_whale_side_weighted_score_sum",
    "trusted_whale_side_weighted_buy_pressure",
    "trusted_whale_side_weighted_sell_pressure",
    "trusted_whale_side_weighted_net_pressure",
    "trusted_whale_side_weighted_net_pressure_per_side_notional",
    "trusted_whale_side_weighted_net_pressure_per_total_notional",
    "trusted_whale_side_weighted_net_pressure_per_market_liquidity",
    "trusted_whale_side_weighted_net_pressure_per_trusted_whale",
    "trusted_whale_side_entry_exit_gap",
    "trusted_whale_side_avg_trades_per_active_day",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_partial_exit_count",
    "trusted_whale_side_full_exit_count",
    "trusted_whale_side_unmatched_sell_count",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_avg_open_holding_hours",
    "trusted_whale_side_realized_pnl",
    "trusted_whale_side_realized_roi",
    "trusted_whale_side_avg_exit_profit",
    *RECENT_TRUSTED_WHALE_FEATURE_COLUMNS,
    "whale_vs_crowd_side_net_notional_gap",
    "trusted_whale_vs_crowd_side_net_notional_gap",
    "top_whale_side_notional_share",
    "top_trusted_whale_side_notional_share",
    "first_whale_trade_age_side_hours",
    "last_whale_trade_age_side_hours",
    "first_trusted_whale_trade_age_side_hours",
    "last_trusted_whale_trade_age_side_hours",
    "price_gap_side_minus_opposite",
    "price_abs_distance_from_even",
    "last_trade_age_hours",
    "last_trade_age_side_hours",
    "last_trade_age_opposite_hours",
    "trade_density_per_day",
)

COVERAGE_FEATURE_COLUMNS = (
    "has_any_trade_before_cutoff",
    "has_both_outcomes_before_cutoff",
    "side_has_trade_before_cutoff",
    "opposite_has_trade_before_cutoff",
    "side_price_observed",
    "opposite_price_observed",
)

STATIC_METADATA_FEATURE_COLUMNS = (
    "side_position_feature",
    "question_char_length",
    "question_token_count",
    "event_title_char_length",
    "event_title_token_count",
    "event_description_char_length",
    "event_description_token_count",
    "event_category_token_count",
    "event_tag_count",
    "event_tag_token_count",
    "market_slug_token_count",
    "event_slug_token_count",
    "question_title_token_overlap",
    "question_description_token_overlap",
    "question_category_token_overlap",
    "question_tag_token_overlap",
    "question_side_token_overlap",
    "question_opposite_token_overlap",
    "side_category_token_overlap",
    "opposite_side_category_token_overlap",
    "side_tag_token_overlap",
    "opposite_side_tag_token_overlap",
    "side_label_char_length",
    "side_label_token_count",
    "opposite_side_label_char_length",
    "opposite_side_label_token_count",
    "side_label_is_yes",
    "side_label_is_no",
    "opposite_label_is_yes",
    "opposite_label_is_no",
    "question_has_number",
    "question_digit_count",
    "question_has_percent",
    "question_has_dollar",
    "question_has_more_than",
    "question_has_less_than",
    "question_has_date_reference",
    "question_starts_with_who_or_which",
    "question_title_exact_match",
    "market_volume_log1p",
    "market_liquidity_log1p",
    "prior_side_label_count",
    "prior_side_label_win_rate",
    "prior_label_pair_count",
    "prior_label_pair_side_win_rate",
    "prior_question_pattern_count",
    "prior_question_pattern_side_win_rate",
    "prior_side_position_count",
    "prior_side_position_win_rate",
    "prior_event_category_count",
    "prior_event_category_side_win_rate",
    "prior_question_opening_count",
    "prior_question_opening_side_win_rate",
    "prior_category_pattern_count",
    "prior_category_pattern_side_win_rate",
)

WHALE_FEATURE_COLUMNS = (
    "whale_distinct_users",
    "trusted_whale_distinct_users",
    "whale_trade_share",
    "trusted_whale_trade_share",
    "whale_notional_share",
    "trusted_whale_notional_share",
    "whale_buy_trade_count_total",
    "whale_sell_trade_count_total",
    *SCORED_WHALE_TOTAL_PRESSURE_FEATURE_COLUMNS,
    "trusted_whale_buy_trade_count_total",
    "trusted_whale_sell_trade_count_total",
    "trusted_whale_buy_sell_ratio_total",
    "trusted_whale_weighted_score_sum_total",
    "trusted_whale_weighted_net_pressure_total",
    "trusted_whale_weighted_net_pressure_total_per_total_notional",
    "trusted_whale_weighted_net_pressure_total_per_market_liquidity",
    "trusted_whale_weighted_net_pressure_total_per_trusted_whale",
    "whale_side_trade_share",
    "trusted_whale_side_trade_share",
    "whale_side_notional_share",
    "trusted_whale_side_notional_share",
    "whale_side_buy_notional_share",
    "whale_side_sell_notional_share",
    "trusted_whale_side_buy_notional_share",
    "trusted_whale_side_sell_notional_share",
    "whale_side_net_notional_share",
    *SCORED_WHALE_SIDE_PRESSURE_FEATURE_COLUMNS,
    "trusted_whale_side_net_notional_share",
    "trusted_whale_side_buy_trade_count",
    "trusted_whale_side_sell_trade_count",
    "trusted_whale_side_buy_sell_ratio",
    "trusted_whale_side_weighted_score_sum",
    "trusted_whale_side_weighted_buy_pressure",
    "trusted_whale_side_weighted_sell_pressure",
    "trusted_whale_side_weighted_net_pressure",
    "trusted_whale_side_weighted_net_pressure_per_side_notional",
    "trusted_whale_side_weighted_net_pressure_per_total_notional",
    "trusted_whale_side_weighted_net_pressure_per_market_liquidity",
    "trusted_whale_side_weighted_net_pressure_per_trusted_whale",
    "trusted_whale_side_entry_exit_gap",
    "trusted_whale_side_avg_trades_per_active_day",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_partial_exit_count",
    "trusted_whale_side_full_exit_count",
    "trusted_whale_side_unmatched_sell_count",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_avg_open_holding_hours",
    "trusted_whale_side_realized_pnl",
    "trusted_whale_side_realized_roi",
    "trusted_whale_side_avg_exit_profit",
    *RECENT_TRUSTED_WHALE_FEATURE_COLUMNS,
    "whale_vs_crowd_side_net_notional_gap",
    "trusted_whale_vs_crowd_side_net_notional_gap",
    "top_whale_side_notional_share",
    "top_trusted_whale_side_notional_share",
    "first_whale_trade_age_side_hours",
    "last_whale_trade_age_side_hours",
    "first_trusted_whale_trade_age_side_hours",
    "last_trusted_whale_trade_age_side_hours",
)

IDENTIFIER_COLUMNS = (
    "dataset_version",
    "condition_ref",
    "event_id",
    "event_title",
    "event_category",
    "event_slug",
    "market_contract_id",
    "market_slug",
    "question",
    "outcome_a_label",
    "outcome_b_label",
    "side_label",
    "opposite_side_label",
    "side_is_outcome_a",
    "observation_time",
    "market_start_time",
    "market_end_time",
)

AUDIT_COLUMNS = (
    "winning_outcome_label",
    "resolution_source",
    "resolution_confidence",
    "resolution_time",
    "label_side_wins",
)

ANALYSIS_COLUMNS = (
    PRICE_BASELINE_COLUMN,
    RESOLUTION_EDGE_COLUMN,
    "future_price_side_12h",
    "future_price_delta_12h",
    "future_price_up_12h",
    "future_price_observed_12h",
    "future_window_reaches_resolution_12h",
    "future_price_side_24h",
    "future_price_delta_24h",
    "future_price_up_24h",
    "future_price_observed_24h",
    "future_window_reaches_resolution_24h",
)

CSV_COLUMNS = IDENTIFIER_COLUMNS + FEATURE_COLUMNS + ANALYSIS_COLUMNS + AUDIT_COLUMNS


CONDITION_METADATA_SQL = text(
    """
    WITH event_tag_rollup AS (
      SELECT
        mtm.event_id,
        COUNT(DISTINCT mt.tag_id) AS event_tag_count,
        STRING_AGG(
          DISTINCT COALESCE(NULLIF(mt.tag_slug, ''), mt.tag_label),
          ' '
          ORDER BY COALESCE(NULLIF(mt.tag_slug, ''), mt.tag_label)
        ) AS event_tag_text
      FROM analytics.market_tag_map mtm
      JOIN analytics.market_tag mt
        ON mt.tag_id = mtm.tag_id
      GROUP BY mtm.event_id
    ),
    ranked_conditions AS (
      SELECT
        mc.condition_ref,
        me.event_id,
        me.title AS event_title,
        COALESCE(me.description, '') AS event_description,
        COALESCE(me.category, '') AS event_category,
        me.slug AS event_slug,
        mc.market_contract_id,
        mc.market_slug,
        mc.question,
        mc.outcome_a_label,
        mc.outcome_b_label,
        COALESCE(mc.volume, me.volume, 0) AS market_volume,
        COALESCE(mc.liquidity, me.liquidity, 0) AS market_liquidity,
        COALESCE(etr.event_tag_count, 0) AS event_tag_count,
        COALESCE(etr.event_tag_text, '') AS event_tag_text,
        COALESCE(mc.start_time, me.start_time) AS market_start_time,
        COALESCE(mc.end_time, me.end_time, me.closed_time) AS market_end_time,
        ROW_NUMBER() OVER (
          PARTITION BY mc.condition_ref
          ORDER BY COALESCE(mc.end_time, me.end_time, me.closed_time) DESC, mc.market_contract_id DESC
        ) AS rn
      FROM analytics.market_contract mc
      JOIN analytics.market_event me
        ON me.event_id = mc.event_id
      LEFT JOIN event_tag_rollup etr
        ON etr.event_id = me.event_id
      JOIN analytics.platform p
        ON p.platform_id = mc.platform_id
      WHERE p.platform_name = 'polymarket'
        AND mc.is_closed = TRUE
        AND mc.condition_ref IS NOT NULL
        AND mc.outcome_a_label IS NOT NULL
        AND mc.outcome_b_label IS NOT NULL
        AND COALESCE(mc.start_time, me.start_time) IS NOT NULL
        AND COALESCE(mc.end_time, me.end_time, me.closed_time) IS NOT NULL
    )
    SELECT
      condition_ref,
      event_id,
      event_title,
      event_description,
      event_category,
      event_slug,
      market_contract_id,
      market_slug,
      question,
      outcome_a_label,
      outcome_b_label,
      market_volume,
      market_liquidity,
      event_tag_count,
      event_tag_text,
      market_start_time,
      market_end_time
    FROM ranked_conditions
    WHERE rn = 1
    ORDER BY market_end_time ASC, condition_ref ASC
    """
)


TRANSACTION_SQL = text(
    """
    SELECT
      tf.transaction_id,
      mc.condition_ref,
      tf.market_contract_id,
      tf.user_id,
      tf.side,
      tf.outcome_label,
      tf.transaction_time,
      COALESCE(tf.shares, 0) AS shares,
      COALESCE(tf.notional_value, 0) AS notional_value,
      COALESCE(tf.price, 0) AS price
    FROM analytics.transaction_fact tf
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = 'polymarket'
      AND mc.condition_ref IS NOT NULL
      AND tf.outcome_label IS NOT NULL
      AND tf.side IN ('buy', 'sell')
    ORDER BY tf.transaction_time, tf.transaction_id
    """
)


USER_ACCOUNT_SQL = text(
    """
    SELECT
      ua.user_id,
      ua.platform_id,
      p.platform_name,
      ua.external_user_ref,
      ua.is_likely_insider
    FROM analytics.user_account ua
    JOIN analytics.platform p
      ON p.platform_id = ua.platform_id
    WHERE p.platform_name = 'polymarket'
    """
)


@dataclass
class OutcomeState:
    """Aggregate point-in-time trade features for one outcome."""

    trade_count: int = 0
    distinct_users: set[int] | None = None
    total_notional: float = 0.0
    buy_notional: float = 0.0
    sell_notional: float = 0.0
    total_shares: float = 0.0
    price_sum: float = 0.0
    min_price: float | None = None
    max_price: float | None = None
    last_price: float | None = None
    last_trade_at: datetime | None = None
    whale_trade_count: int = 0
    trusted_whale_trade_count: int = 0
    whale_buy_trade_count: int = 0
    whale_sell_trade_count: int = 0
    trusted_whale_buy_trade_count: int = 0
    trusted_whale_sell_trade_count: int = 0
    whale_notional: float = 0.0
    trusted_whale_notional: float = 0.0
    whale_buy_notional: float = 0.0
    whale_sell_notional: float = 0.0
    trusted_whale_buy_notional: float = 0.0
    trusted_whale_sell_notional: float = 0.0
    whale_user_notional: dict[int, float] | None = None
    trusted_whale_user_notional: dict[int, float] | None = None
    trusted_whale_user_ids: set[int] | None = None
    whale_weighted_score_sum: float = 0.0
    whale_weighted_buy_pressure: float = 0.0
    whale_weighted_sell_pressure: float = 0.0
    trusted_whale_weighted_score_sum: float = 0.0
    trusted_whale_weighted_buy_pressure: float = 0.0
    trusted_whale_weighted_sell_pressure: float = 0.0
    first_whale_trade_at: datetime | None = None
    last_whale_trade_at: datetime | None = None
    first_trusted_whale_trade_at: datetime | None = None
    last_trusted_whale_trade_at: datetime | None = None

    def __post_init__(self) -> None:
        """Initialize mutable defaults safely."""
        if self.distinct_users is None:
            self.distinct_users = set()
        if self.whale_user_notional is None:
            self.whale_user_notional = {}
        if self.trusted_whale_user_notional is None:
            self.trusted_whale_user_notional = {}
        if self.trusted_whale_user_ids is None:
            self.trusted_whale_user_ids = set()


@dataclass
class HistoricalMetricState:
    """Cumulative user metrics available at one observation cutoff."""

    trade_count: int = 0
    distinct_markets: set[int] | None = None
    active_trade_days: set[date] | None = None
    total_notional: float = 0.0

    def __post_init__(self) -> None:
        """Initialize mutable defaults safely."""
        if self.distinct_markets is None:
            self.distinct_markets = set()
        if self.active_trade_days is None:
            self.active_trade_days = set()


@dataclass
class ResolvedContribution:
    """One user-condition resolved contribution."""

    resolved_market_count: int = 0
    winning_market_count: int = 0
    realized_pnl: float = 0.0
    invested_total: float = 0.0
    excluded_market_count: int = 0


@dataclass
class ResolvedAggregateState:
    """Incremental resolved-performance totals for one user."""

    resolved_market_count: int = 0
    winning_market_count: int = 0
    realized_pnl: float = 0.0
    invested_total: float = 0.0
    excluded_market_count: int = 0


@dataclass
class HistoricalWhaleSnapshot:
    """Historical whale membership and user-level weights at one cutoff."""

    whale_user_ids: set[int]
    trusted_whale_user_ids: set[int]
    weighted_score_by_user: dict[int, float]
    trades_per_active_day_by_user: dict[int, float]


@dataclass
class PositionBehaviorSummary:
    """Entry/exit behavior reconstructed from matched buy/sell lots."""

    entry_trade_count: int = 0
    exit_trade_count: int = 0
    partial_exit_count: int = 0
    full_exit_count: int = 0
    unmatched_sell_count: int = 0
    avg_holding_hours: float = 0.0
    avg_open_holding_hours: float = 0.0
    realized_pnl: float = 0.0
    realized_roi: float = 0.0
    avg_exit_profit: float = 0.0


def _normalized_label(value: str | None) -> str | None:
    """Normalize outcome labels for stable matching."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _iso(value: datetime | None) -> str:
    """Serialize optional datetimes for CSV/JSON output."""
    if value is None:
        return ""
    return value.isoformat()


def _hours_between(later: datetime | None, earlier: datetime | None) -> float:
    """Return a non-negative hour delta between two timestamps."""
    if later is None or earlier is None:
        return 0.0
    return round(max((later - earlier).total_seconds() / 3600.0, 0.0), 6)


def _safe_divide(numerator: float, denominator: float) -> float:
    """Return a rounded division result or zero when the denominator is empty."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 8)


def _safe_average(values: list[float]) -> float:
    """Return a rounded average for a numeric list."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 8)


def _bounded_unit(value: float) -> float:
    """Clamp a score-like value to the 0..1 range."""
    return round(min(max(float(value), 0.0), 1.0), 8)


def _summarize_position_behavior(
    trades: list[dict[str, Any]],
    *,
    user_ids: set[int],
    outcome_label: str,
    observation_time: datetime,
) -> PositionBehaviorSummary:
    """Reconstruct entry/exit behavior from matched buy and sell lots."""
    if not user_ids:
        return PositionBehaviorSummary()

    normalized_outcome = _normalized_label(outcome_label)
    open_lots_by_user: dict[int, list[dict[str, float | datetime]]] = {}
    entry_trade_count = 0
    exit_trade_count = 0
    partial_exit_count = 0
    full_exit_count = 0
    unmatched_sell_count = 0
    realized_pnl = 0.0
    matched_basis = 0.0
    exit_profits: list[float] = []
    holding_hours: list[float] = []

    for trade in sorted(trades, key=lambda item: (item["transaction_time"], item["transaction_id"])):
        user_id = int(trade["user_id"])
        if user_id not in user_ids or _normalized_label(str(trade["outcome_label"])) != normalized_outcome:
            continue
        shares = float(trade["shares"] or 0.0)
        notional_value = float(trade["notional_value"] or 0.0)
        if shares <= 0:
            continue
        trade_time = trade["transaction_time"]
        lots = open_lots_by_user.setdefault(user_id, [])
        if trade["side"] == "buy":
            entry_trade_count += 1
            lots.append(
                {
                    "shares": shares,
                    "unit_cost": (notional_value / shares) if shares > 0 else 0.0,
                    "opened_at": trade_time,
                }
            )
            continue

        remaining_sell_shares = shares
        sell_unit_price = (notional_value / shares) if shares > 0 else 0.0
        before_open_shares = sum(float(lot["shares"]) for lot in lots)
        if before_open_shares <= 1e-9:
            unmatched_sell_count += 1
            continue

        exit_trade_count += 1
        trade_exit_profit = 0.0
        while remaining_sell_shares > 1e-9 and lots:
            lot = lots[0]
            lot_shares = float(lot["shares"])
            matched_shares = min(lot_shares, remaining_sell_shares)
            basis = matched_shares * float(lot["unit_cost"])
            proceeds = matched_shares * sell_unit_price
            matched_basis += basis
            trade_exit_profit += proceeds - basis
            holding_hours.append(_hours_between(trade_time, lot["opened_at"]))
            lot["shares"] = lot_shares - matched_shares
            remaining_sell_shares -= matched_shares
            if float(lot["shares"]) <= 1e-9:
                lots.pop(0)

        if remaining_sell_shares > 1e-9:
            unmatched_sell_count += 1
        after_open_shares = sum(float(lot["shares"]) for lot in lots)
        if after_open_shares <= 1e-9:
            full_exit_count += 1
        else:
            partial_exit_count += 1
        realized_pnl += trade_exit_profit
        exit_profits.append(trade_exit_profit)

    open_holding_hours: list[float] = []
    for lots in open_lots_by_user.values():
        for lot in lots:
            if float(lot["shares"]) > 1e-9:
                open_holding_hours.append(_hours_between(observation_time, lot["opened_at"]))

    return PositionBehaviorSummary(
        entry_trade_count=entry_trade_count,
        exit_trade_count=exit_trade_count,
        partial_exit_count=partial_exit_count,
        full_exit_count=full_exit_count,
        unmatched_sell_count=unmatched_sell_count,
        avg_holding_hours=_safe_average(holding_hours),
        avg_open_holding_hours=_safe_average(open_holding_hours),
        realized_pnl=round(realized_pnl, 8),
        realized_roi=_safe_divide(realized_pnl, matched_basis),
        avg_exit_profit=_safe_average(exit_profits),
    )


def _recent_trusted_whale_pressure_features(
    trades: list[dict[str, Any]],
    *,
    trusted_whale_user_ids: set[int],
    weighted_score_by_user: dict[int, float],
    outcome_label: str,
    observation_time: datetime,
    side_total_notional: float,
) -> dict[str, float]:
    """Return recent trusted-whale entry/exit pressure features before a cutoff."""
    normalized_outcome = _normalized_label(outcome_label)
    features: dict[str, float] = {}
    for window_hours in RECENT_WHALE_WINDOWS_HOURS:
        window_start = observation_time - timedelta(hours=window_hours)
        distinct_users: set[int] = set()
        entry_trade_count = 0
        exit_trade_count = 0
        entry_notional = 0.0
        exit_notional = 0.0
        weighted_entry_pressure = 0.0
        weighted_exit_pressure = 0.0
        decay_weighted_entry_pressure = 0.0
        decay_weighted_exit_pressure = 0.0

        for trade in trades:
            trade_time = trade["transaction_time"]
            if trade_time > observation_time or trade_time < window_start:
                continue
            user_id = int(trade["user_id"])
            if user_id not in trusted_whale_user_ids:
                continue
            if _normalized_label(str(trade["outcome_label"])) != normalized_outcome:
                continue

            notional_value = float(trade["notional_value"] or 0.0)
            if notional_value <= 0:
                continue
            distinct_users.add(user_id)
            whale_weight = float(weighted_score_by_user.get(user_id, 0.0))
            age_hours = _hours_between(observation_time, trade_time)
            recency_decay = math.exp(-age_hours / max(float(window_hours), 1.0))
            weighted_pressure = whale_weight * notional_value
            decay_weighted_pressure = weighted_pressure * recency_decay
            if trade["side"] == "buy":
                entry_trade_count += 1
                entry_notional += notional_value
                weighted_entry_pressure += weighted_pressure
                decay_weighted_entry_pressure += decay_weighted_pressure
            else:
                exit_trade_count += 1
                exit_notional += notional_value
                weighted_exit_pressure += weighted_pressure
                decay_weighted_exit_pressure += decay_weighted_pressure

        net_notional = entry_notional - exit_notional
        weighted_net_pressure = weighted_entry_pressure - weighted_exit_pressure
        decay_weighted_net_pressure = decay_weighted_entry_pressure - decay_weighted_exit_pressure
        prefix = f"trusted_whale_side_recent_"
        suffix = f"_{window_hours}h"
        features[f"{prefix}trade_count{suffix}"] = entry_trade_count + exit_trade_count
        features[f"{prefix}distinct_users{suffix}"] = len(distinct_users)
        features[f"{prefix}entry_trade_count{suffix}"] = entry_trade_count
        features[f"{prefix}exit_trade_count{suffix}"] = exit_trade_count
        features[f"{prefix}entry_notional{suffix}"] = round(entry_notional, 8)
        features[f"{prefix}exit_notional{suffix}"] = round(exit_notional, 8)
        features[f"{prefix}net_notional{suffix}"] = round(net_notional, 8)
        features[f"{prefix}net_notional_share{suffix}"] = _safe_divide(net_notional, side_total_notional)
        features[f"{prefix}weighted_entry_pressure{suffix}"] = round(weighted_entry_pressure, 8)
        features[f"{prefix}weighted_exit_pressure{suffix}"] = round(weighted_exit_pressure, 8)
        features[f"{prefix}weighted_net_pressure{suffix}"] = round(weighted_net_pressure, 8)
        features[f"{prefix}decay_weighted_entry_pressure{suffix}"] = round(decay_weighted_entry_pressure, 8)
        features[f"{prefix}decay_weighted_exit_pressure{suffix}"] = round(decay_weighted_exit_pressure, 8)
        features[f"{prefix}decay_weighted_net_pressure{suffix}"] = round(decay_weighted_net_pressure, 8)
        features[f"{prefix}entry_exit_ratio{suffix}"] = _safe_divide(entry_trade_count, max(exit_trade_count, 1))

    return features


TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
DATE_REFERENCE_PATTERN = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|"
    r"20\d{2}|19\d{2}|q[1-4]|quarter"
    r")\b",
    re.IGNORECASE,
)


def _tokenize_text(value: str | None) -> list[str]:
    """Return lowercase alphanumeric tokens from free text."""
    return TOKEN_PATTERN.findall(str(value or "").lower())


def _token_key(value: str | None, *, default: str) -> str:
    """Return a normalized token key for historical buckets."""
    tokens = _tokenize_text(value)
    if not tokens:
        return default
    return "_".join(tokens)


def _token_overlap_ratio(left_tokens: list[str], right_tokens: list[str]) -> float:
    """Return a Jaccard-style overlap ratio for two token sets."""
    left_set = set(left_tokens)
    right_set = set(right_tokens)
    if not left_set and not right_set:
        return 0.0
    return _safe_divide(float(len(left_set & right_set)), float(len(left_set | right_set)))


def _question_has_date_reference(question_text: str) -> int:
    """Return whether the question text includes a date-like reference."""
    return int(bool(DATE_REFERENCE_PATTERN.search(question_text)))


def _question_opening_key(question_text: str) -> str:
    """Return a coarse question-opening key for broader cold-start priors."""
    tokens = _tokenize_text(question_text)
    if not tokens:
        return "unknown_opening"
    return "_".join(tokens[:2])


def _question_pattern_key(
    question_text: str,
    outcome_a_label: str,
    outcome_b_label: str,
) -> str:
    """Return a coarse question template key for cold-start priors."""
    normalized_question = str(question_text).strip().lower()
    labels = {str(outcome_a_label).strip().lower(), str(outcome_b_label).strip().lower()}
    if labels == {"yes", "no"}:
        if any(marker in normalized_question for marker in ("more than", "less than", "over ", "under ", "at least", "at most")):
            return "yes_no_threshold"
        if _question_has_date_reference(normalized_question) or any(
            marker in normalized_question for marker in (" by ", " before ", " after ", " during ")
        ):
            return "yes_no_deadline"
        return "yes_no"
    if normalized_question.startswith("who ") or normalized_question.startswith("which "):
        return "entity_choice"
    if any(marker in normalized_question for marker in ("more than", "less than", "over ", "under ", "at least", "at most")):
        return "threshold"
    return "other"


def _safe_log1p(value: float | int | str | None) -> float:
    """Return a rounded log1p transform for numeric metadata."""
    numeric_value = float(value or 0.0)
    return round(math.log1p(max(numeric_value, 0.0)), 8)


def _prior_stat_summary(
    stats: dict[Any, dict[str, float]],
    key: Any,
    *,
    smoothing_count: float = 4.0,
    prior_mean: float = 0.5,
) -> tuple[int, float]:
    """Return prior count and a smoothed neutral-default win rate for one key."""
    entry = stats.get(key)
    if not entry or entry["count"] <= 0:
        return 0, 0.5
    count = float(entry["count"])
    smoothed_rate = (float(entry["wins"]) + (float(prior_mean) * float(smoothing_count))) / (
        count + float(smoothing_count)
    )
    return int(count), round(smoothed_rate, 8)


def _update_prior_stat(
    stats: dict[Any, dict[str, float]],
    key: Any,
    *,
    did_win: int,
) -> None:
    """Increment one prior-stat bucket."""
    entry = stats.setdefault(key, {"count": 0.0, "wins": 0.0})
    entry["count"] += 1.0
    entry["wins"] += float(did_win)


def _neutral_price() -> float:
    """Return the neutral fallback price used when no point-in-time trade exists."""
    return 0.5


def _average_price(state: OutcomeState) -> float:
    """Return the observed average price or a neutral fallback for sparse snapshots."""
    if state.trade_count <= 0:
        return _neutral_price()
    return _safe_divide(state.price_sum, state.trade_count)


def _observed_price(state: OutcomeState, *, fallback: float) -> float:
    """Return the last observed price or the provided fallback when no trade exists."""
    if state.last_price is None:
        return round(float(fallback), 8)
    return round(float(state.last_price), 8)


def _observed_extreme(value: float | None, *, fallback: float) -> float:
    """Return the observed min/max price or the fallback price when absent."""
    if value is None:
        return round(float(fallback), 8)
    return round(float(value), 8)


def _last_trade_age_hours(
    observation_time: datetime,
    trade_time: datetime | None,
    *,
    market_age_hours: float,
) -> float:
    """Return trade age or market age when the snapshot predates all known trades."""
    if trade_time is None:
        return round(float(market_age_hours), 6)
    return _hours_between(observation_time, trade_time)


def _future_price_summary(
    *,
    condition_transactions: list[dict[str, Any]],
    side_label: str,
    observation_time: datetime,
    market_end_time: datetime,
    side_last_price: float,
    side_label_wins: int,
    window_hours: int,
) -> dict[str, float]:
    """Return the side price movement over a future 12h/24h window."""
    target_time = observation_time + timedelta(hours=window_hours)
    reaches_resolution = int(target_time >= market_end_time)
    capped_target_time = min(target_time, market_end_time)
    normalized_side = _normalized_label(side_label)
    future_price = float(side_label_wins) if reaches_resolution else float(side_last_price)
    future_observed = reaches_resolution

    for trade in condition_transactions:
        if trade["transaction_time"] <= observation_time or trade["transaction_time"] > capped_target_time:
            continue
        if _normalized_label(str(trade["outcome_label"])) != normalized_side:
            continue
        future_price = float(trade["price"] or future_price)
        future_observed = 1

    return {
        f"future_price_side_{window_hours}h": round(future_price, 8),
        f"future_price_delta_{window_hours}h": round(future_price - float(side_last_price), 8),
        f"future_price_up_{window_hours}h": int(future_price > float(side_last_price)),
        f"future_price_observed_{window_hours}h": int(future_observed),
        f"future_window_reaches_resolution_{window_hours}h": reaches_resolution,
    }


def _dataset_leakage_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return simple target-leakage diagnostics for exported binary columns."""
    if not rows:
        return {
            "row_count": 0,
            "side_position_target_agreement": None,
            "side_is_outcome_a_target_agreement": None,
            "max_binary_feature_target_agreement": None,
            "flagged_binary_features": [],
        }

    target_values = [int(row[TARGET_COLUMN]) for row in rows]

    def agreement_for(column: str) -> float | None:
        values: list[int] = []
        for row in rows:
            if column not in row:
                return None
            value = row[column]
            try:
                numeric_value = int(float(value))
            except (TypeError, ValueError):
                return None
            if numeric_value not in {0, 1}:
                return None
            values.append(numeric_value)
        matches = sum(int(value == target) for value, target in zip(values, target_values, strict=True))
        return round(matches / len(rows), 8)

    flagged_binary_features: list[dict[str, Any]] = []
    max_binary_feature_target_agreement: dict[str, Any] | None = None
    for column in FEATURE_COLUMNS:
        agreement = agreement_for(column)
        if agreement is None:
            continue
        distance_from_random = abs(agreement - 0.5)
        candidate = {
            "feature": column,
            "target_agreement": agreement,
            "distance_from_random": round(distance_from_random, 8),
        }
        if max_binary_feature_target_agreement is None or distance_from_random > float(
            max_binary_feature_target_agreement["distance_from_random"]
        ):
            max_binary_feature_target_agreement = candidate
        if agreement >= 0.95 or agreement <= 0.05:
            flagged_binary_features.append(candidate)

    return {
        "row_count": len(rows),
        "side_position_target_agreement": agreement_for("side_position_feature"),
        "side_is_outcome_a_target_agreement": agreement_for("side_is_outcome_a"),
        "max_binary_feature_target_agreement": max_binary_feature_target_agreement,
        "flagged_binary_features": flagged_binary_features,
    }


def _parse_horizons(horizon_hours: tuple[int, ...] | list[int] | None) -> tuple[int, ...]:
    """Return sorted positive unique horizon hours."""
    if not horizon_hours:
        return DEFAULT_HORIZON_HOURS
    values = sorted({int(value) for value in horizon_hours if int(value) > 0}, reverse=True)
    return tuple(values)


def _load_polymarket_user_lookup(session: Session) -> dict[int, dict[str, Any]]:
    """Return canonical Polymarket user metadata keyed by user id."""
    rows = session.execute(USER_ACCOUNT_SQL).mappings().all()
    return {int(row["user_id"]): dict(row) for row in rows}


def _compute_condition_contribution(
    outcome_map: dict[str, dict[str, float]],
    winning_outcome: str | None,
) -> ResolvedContribution:
    """Return one user-condition contribution to resolved performance totals."""
    if not winning_outcome:
        return ResolvedContribution()
    if any(values["sold_shares"] > values["bought_shares"] + 1e-9 for values in outcome_map.values()):
        return ResolvedContribution(excluded_market_count=1)

    condition_cash_flow = 0.0
    condition_final_value = 0.0
    condition_invested = 0.0
    for outcome_label, values in outcome_map.items():
        net_shares = max(values["bought_shares"] - values["sold_shares"], 0.0)
        condition_cash_flow += values["sell_notional"] - values["buy_notional"]
        condition_invested += values["buy_notional"]
        if outcome_label == winning_outcome:
            condition_final_value += net_shares

    condition_realized_pnl = condition_cash_flow + condition_final_value
    return ResolvedContribution(
        resolved_market_count=1,
        winning_market_count=1 if condition_realized_pnl > 0 else 0,
        realized_pnl=round(condition_realized_pnl, 8),
        invested_total=round(condition_invested, 8),
        excluded_market_count=0,
    )


def _compute_open_condition_exposure(outcome_map: dict[str, dict[str, float]]) -> float:
    """Approximate open-condition exposure from net shares valued at average buy price."""
    exposure = 0.0
    for values in outcome_map.values():
        net_shares = max(values["bought_shares"] - values["sold_shares"], 0.0)
        avg_buy_price = (values["buy_notional"] / values["bought_shares"]) if values["bought_shares"] > 0 else 0.0
        exposure += net_shares * avg_buy_price
    return round(exposure, 8)


def _apply_resolved_contribution_delta(
    aggregate: ResolvedAggregateState,
    *,
    old: ResolvedContribution | None,
    new: ResolvedContribution | None,
) -> None:
    """Apply one user-condition resolved contribution delta to the aggregate."""
    if old is not None:
        aggregate.resolved_market_count -= old.resolved_market_count
        aggregate.winning_market_count -= old.winning_market_count
        aggregate.realized_pnl = round(aggregate.realized_pnl - old.realized_pnl, 8)
        aggregate.invested_total = round(aggregate.invested_total - old.invested_total, 8)
        aggregate.excluded_market_count -= old.excluded_market_count
    if new is not None:
        aggregate.resolved_market_count += new.resolved_market_count
        aggregate.winning_market_count += new.winning_market_count
        aggregate.realized_pnl = round(aggregate.realized_pnl + new.realized_pnl, 8)
        aggregate.invested_total = round(aggregate.invested_total + new.invested_total, 8)
        aggregate.excluded_market_count += new.excluded_market_count


def _resolved_performance_snapshot(
    aggregate_by_user: dict[int, ResolvedAggregateState],
) -> dict[int, ResolvedUserPerformance]:
    """Convert incremental aggregate state into the scoring dataclass contract."""
    snapshot: dict[int, ResolvedUserPerformance] = {}
    for user_id, aggregate in aggregate_by_user.items():
        if aggregate.resolved_market_count == 0 and aggregate.excluded_market_count == 0:
            continue
        realized_roi = (aggregate.realized_pnl / aggregate.invested_total) if aggregate.invested_total > 0 else 0.0
        snapshot[user_id] = ResolvedUserPerformance(
            user_id=user_id,
            resolved_market_count=aggregate.resolved_market_count,
            winning_market_count=aggregate.winning_market_count,
            realized_pnl=round(aggregate.realized_pnl, 8),
            realized_roi=round(realized_roi, 8),
            excluded_market_count=aggregate.excluded_market_count,
        )
    return snapshot


def _build_historical_whale_sets_by_observation(
    session: Session,
    *,
    observation_times: list[datetime],
    transactions: list[dict[str, Any]],
    resolved_outcomes: dict[str, str],
    condition_end_by_ref: dict[str, datetime],
    whale_weight_config: WhaleWeightConfig,
) -> dict[datetime, HistoricalWhaleSnapshot]:
    """Return whale membership and weighted user scores for each observation cutoff."""
    user_lookup = _load_polymarket_user_lookup(session)
    metric_state_by_user: dict[int, HistoricalMetricState] = {}
    condition_state_by_user: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    condition_user_ids: dict[str, set[int]] = {}
    resolved_aggregate_by_user: dict[int, ResolvedAggregateState] = {}
    resolved_contribution_by_user_condition: dict[tuple[int, str], ResolvedContribution] = {}
    open_exposure_by_user: dict[int, float] = {}
    open_exposure_by_user_condition: dict[tuple[int, str], float] = {}
    historical_sets: dict[datetime, HistoricalWhaleSnapshot] = {}
    previous_snapshot = HistoricalWhaleSnapshot(
        whale_user_ids=set(),
        trusted_whale_user_ids=set(),
        weighted_score_by_user={},
        trades_per_active_day_by_user={},
    )
    activated_condition_refs: set[str] = set()
    ordered_conditions = sorted(condition_end_by_ref.items(), key=lambda item: item[1])
    activation_index = 0
    transaction_index = 0

    for observation_time in observation_times:
        state_changed = False
        while transaction_index < len(transactions):
            trade = transactions[transaction_index]
            if trade["transaction_time"] > observation_time:
                break

            user_id = int(trade["user_id"])
            condition_ref = str(trade["condition_ref"])
            state = metric_state_by_user.setdefault(user_id, HistoricalMetricState())
            state.trade_count += 1
            state.distinct_markets.add(int(trade["market_contract_id"]))
            trade_time = trade["transaction_time"]
            state.active_trade_days.add(trade_time.date())
            state.total_notional += float(trade["notional_value"] or 0.0)
            condition_user_ids.setdefault(condition_ref, set()).add(user_id)
            state_changed = True

            outcome_label = _normalized_label(str(trade["outcome_label"]))
            if outcome_label:
                user_condition_map = condition_state_by_user.setdefault(user_id, {})
                condition_bucket = user_condition_map.setdefault(condition_ref, {})
                outcome_bucket = condition_bucket.setdefault(
                    outcome_label,
                    {"bought_shares": 0.0, "sold_shares": 0.0, "buy_notional": 0.0, "sell_notional": 0.0},
                )
                shares = float(trade["shares"] or 0.0)
                notional_value = float(trade["notional_value"] or 0.0)
                if trade["side"] == "buy":
                    outcome_bucket["bought_shares"] += shares
                    outcome_bucket["buy_notional"] += notional_value
                else:
                    outcome_bucket["sold_shares"] += shares
                    outcome_bucket["sell_notional"] += notional_value

                if condition_ref in activated_condition_refs:
                    aggregate = resolved_aggregate_by_user.setdefault(user_id, ResolvedAggregateState())
                    contribution_key = (user_id, condition_ref)
                    old_contribution = resolved_contribution_by_user_condition.get(contribution_key)
                    new_contribution = _compute_condition_contribution(
                        condition_bucket,
                        resolved_outcomes.get(condition_ref),
                    )
                    _apply_resolved_contribution_delta(
                        aggregate,
                        old=old_contribution,
                        new=new_contribution,
                    )
                    resolved_contribution_by_user_condition[contribution_key] = new_contribution
                else:
                    exposure_key = (user_id, condition_ref)
                    old_exposure = open_exposure_by_user_condition.get(exposure_key, 0.0)
                    new_exposure = _compute_open_condition_exposure(condition_bucket)
                    if abs(new_exposure - old_exposure) > 1e-12:
                        open_exposure_by_user[user_id] = round(
                            open_exposure_by_user.get(user_id, 0.0) + (new_exposure - old_exposure),
                            8,
                        )
                        open_exposure_by_user_condition[exposure_key] = new_exposure

            transaction_index += 1

        while activation_index < len(ordered_conditions):
            condition_ref, condition_end_time = ordered_conditions[activation_index]
            if condition_end_time > observation_time:
                break
            activated_condition_refs.add(condition_ref)
            state_changed = True
            for user_id in condition_user_ids.get(condition_ref, set()):
                exposure_key = (user_id, condition_ref)
                old_exposure = open_exposure_by_user_condition.pop(exposure_key, 0.0)
                if abs(old_exposure) > 1e-12:
                    open_exposure_by_user[user_id] = round(open_exposure_by_user.get(user_id, 0.0) - old_exposure, 8)
                outcome_map = condition_state_by_user.get(user_id, {}).get(condition_ref)
                if not outcome_map:
                    continue
                contribution = _compute_condition_contribution(
                    outcome_map,
                    resolved_outcomes.get(condition_ref),
                )
                resolved_contribution_by_user_condition[(user_id, condition_ref)] = contribution
                aggregate = resolved_aggregate_by_user.setdefault(user_id, ResolvedAggregateState())
                _apply_resolved_contribution_delta(
                    aggregate,
                    old=None,
                    new=contribution,
                )
            activation_index += 1

        if not state_changed:
            historical_sets[observation_time] = previous_snapshot
            continue

        resolved_performance_by_user = _resolved_performance_snapshot(resolved_aggregate_by_user)

        metric_inputs: list[WhaleMetricInput] = []
        for user_id, state in metric_state_by_user.items():
            user_meta = user_lookup.get(user_id)
            if not user_meta:
                continue
            external_user_ref = str(user_meta["external_user_ref"])
            if external_user_ref == UNKNOWN_USER_EXTERNAL_REF:
                continue
            metric_inputs.append(
                WhaleMetricInput(
                    user_id=user_id,
                    platform_id=int(user_meta["platform_id"]),
                    platform_name=str(user_meta["platform_name"]),
                    external_user_ref=external_user_ref,
                    is_likely_insider=bool(user_meta["is_likely_insider"]),
                    sample_trade_count=state.trade_count,
                    distinct_markets=len(state.distinct_markets),
                    active_trade_days=len(state.active_trade_days),
                    total_notional=round(state.total_notional, 8),
                    current_exposure=round(open_exposure_by_user.get(user_id, 0.0), 8),
                )
            )

        scored_results = compute_whale_scores(
            metric_inputs,
            resolved_performance_by_user=resolved_performance_by_user,
        )
        whale_user_ids = {item.metric.user_id for item in scored_results if item.is_whale}
        trusted_whale_user_ids = {item.metric.user_id for item in scored_results if item.is_trusted_whale}
        trades_per_active_day_by_user = {
            user_id: _safe_divide(float(state.trade_count), float(max(len(state.active_trade_days), 1)))
            for user_id, state in metric_state_by_user.items()
        }
        weighted_score_by_user: dict[int, float] = {}
        for item in scored_results:
            frequency_score = _bounded_unit(trades_per_active_day_by_user.get(item.metric.user_id, 0.0) / 10.0)
            weighted_score_by_user[item.metric.user_id] = compute_weighted_whale_score(
                whale_weight_config,
                trust_score=item.trust_score,
                profitability_score=item.profitability_score,
                frequency_score=frequency_score,
            )
        previous_snapshot = HistoricalWhaleSnapshot(
            whale_user_ids=whale_user_ids,
            trusted_whale_user_ids=trusted_whale_user_ids,
            weighted_score_by_user=weighted_score_by_user,
            trades_per_active_day_by_user=trades_per_active_day_by_user,
        )
        historical_sets[observation_time] = previous_snapshot

    return historical_sets


def build_market_snapshot_dataset(
    session: Session,
    *,
    horizon_hours: tuple[int, ...] | list[int] | None = None,
    whale_weight_config_path: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build point-in-time market snapshots for resolved Polymarket conditions."""
    resolved_outcome_details = load_resolved_market_outcome_details(session)
    resolved_outcomes = {
        condition_ref: detail.winning_outcome_label
        for condition_ref, detail in resolved_outcome_details.items()
    }
    whale_weight_config = load_whale_weight_config(whale_weight_config_path)
    horizons = _parse_horizons(horizon_hours)
    metadata_rows = session.execute(CONDITION_METADATA_SQL).mappings().all()
    transactions = [dict(row) for row in session.execute(TRANSACTION_SQL).mappings().all()]

    metadata_by_condition = {
        str(row["condition_ref"]): row
        for row in metadata_rows
        if str(row["condition_ref"]) in resolved_outcomes
    }
    condition_end_by_ref = {
        str(condition_ref): meta["market_end_time"]
        for condition_ref, meta in metadata_by_condition.items()
    }

    observation_times = sorted(
        {
            meta["market_end_time"] - timedelta(hours=horizon)
            for meta in metadata_by_condition.values()
            for horizon in horizons
        }
    )
    historical_whale_sets = _build_historical_whale_sets_by_observation(
        session,
        observation_times=observation_times,
        transactions=transactions,
        resolved_outcomes=resolved_outcomes,
        condition_end_by_ref=condition_end_by_ref,
        whale_weight_config=whale_weight_config,
    )

    transactions_by_condition: dict[str, list[dict[str, Any]]] = {}
    for row in transactions:
        condition_ref = str(row["condition_ref"])
        if condition_ref not in metadata_by_condition:
            continue
        transactions_by_condition.setdefault(condition_ref, []).append(dict(row))

    dataset_rows: list[dict[str, Any]] = []
    horizon_counts = {str(value): 0 for value in horizons}
    excluded_counts = {
        "missing_transactions_before_cutoff": 0,
        "missing_both_outcomes_before_cutoff": 0,
        "missing_resolved_outcome": 0,
    }
    included_sparse_counts = {
        "no_trades_before_cutoff": 0,
        "single_outcome_before_cutoff": 0,
    }
    prior_side_label_stats: dict[str, dict[str, float]] = {}
    prior_label_pair_side_stats: dict[tuple[tuple[str, str], str], dict[str, float]] = {}
    prior_question_pattern_side_stats: dict[tuple[str, str], dict[str, float]] = {}
    prior_side_position_stats: dict[int, dict[str, float]] = {}
    prior_event_category_side_stats: dict[tuple[str, str], dict[str, float]] = {}
    prior_question_opening_side_stats: dict[tuple[str, str], dict[str, float]] = {}
    prior_category_pattern_side_stats: dict[tuple[tuple[str, str], str], dict[str, float]] = {}

    ordered_conditions = sorted(
        metadata_by_condition.items(),
        key=lambda item: (item[1]["market_end_time"], str(item[0])),
    )
    for condition_ref, meta in ordered_conditions:
        resolution_detail = resolved_outcome_details.get(condition_ref)
        winning_outcome = resolution_detail.winning_outcome_label if resolution_detail else None
        if not winning_outcome:
            excluded_counts["missing_resolved_outcome"] += 1
            continue

        outcome_a_label = _normalized_label(str(meta["outcome_a_label"]))
        outcome_b_label = _normalized_label(str(meta["outcome_b_label"]))
        if not outcome_a_label or not outcome_b_label or outcome_a_label == outcome_b_label:
            excluded_counts["missing_resolved_outcome"] += 1
            continue

        condition_transactions = transactions_by_condition.get(condition_ref, [])
        market_start_time = meta["market_start_time"]
        market_end_time = meta["market_end_time"]
        market_duration_hours = _hours_between(market_end_time, market_start_time)
        question_text = str(meta["question"])
        event_title_text = str(meta["event_title"])
        event_description_text = str(meta["event_description"] or "")
        event_category_text = str(meta["event_category"] or "")
        event_tag_text = str(meta["event_tag_text"] or "")
        market_slug_text = str(meta["market_slug"] or "")
        event_slug_text = str(meta["event_slug"] or "")
        market_volume = float(meta["market_volume"] or 0.0)
        market_liquidity = float(meta["market_liquidity"] or 0.0)
        normalized_question_text = question_text.strip().lower()
        normalized_event_title_text = event_title_text.strip().lower()
        question_tokens = _tokenize_text(question_text)
        event_title_tokens = _tokenize_text(event_title_text)
        event_description_tokens = _tokenize_text(event_description_text)
        event_category_tokens = _tokenize_text(event_category_text)
        event_tag_tokens = _tokenize_text(event_tag_text)
        market_slug_tokens = _tokenize_text(market_slug_text)
        event_slug_tokens = _tokenize_text(event_slug_text)
        question_char_length = len(question_text.strip())
        event_title_char_length = len(event_title_text.strip())
        event_description_char_length = len(event_description_text.strip())
        question_token_count = len(question_tokens)
        event_title_token_count = len(event_title_tokens)
        event_description_token_count = len(event_description_tokens)
        event_category_token_count = len(event_category_tokens)
        event_tag_count = int(meta["event_tag_count"] or 0)
        event_tag_token_count = len(event_tag_tokens)
        market_slug_token_count = len(market_slug_tokens)
        event_slug_token_count = len(event_slug_tokens)
        question_title_token_overlap = _token_overlap_ratio(question_tokens, event_title_tokens)
        question_description_token_overlap = _token_overlap_ratio(question_tokens, event_description_tokens)
        question_category_token_overlap = _token_overlap_ratio(question_tokens, event_category_tokens)
        question_tag_token_overlap = _token_overlap_ratio(question_tokens, event_tag_tokens)
        question_has_number = int(any(character.isdigit() for character in question_text))
        question_digit_count = sum(character.isdigit() for character in question_text)
        question_has_percent = int("%" in question_text)
        question_has_dollar = int("$" in question_text)
        question_has_more_than = int(
            any(marker in normalized_question_text for marker in ("more than", "over ", "at least"))
        )
        question_has_less_than = int(
            any(marker in normalized_question_text for marker in ("less than", "under ", "at most"))
        )
        question_has_date_reference = _question_has_date_reference(question_text)
        question_starts_with_who_or_which = int(
            normalized_question_text.startswith("who ") or normalized_question_text.startswith("which ")
        )
        question_title_exact_match = int(
            bool(normalized_question_text)
            and bool(normalized_event_title_text)
            and normalized_question_text == normalized_event_title_text
        )
        question_pattern = _question_pattern_key(question_text, outcome_a_label, outcome_b_label)
        question_opening = _question_opening_key(question_text)
        event_category_key = _token_key(event_category_text, default="uncategorized")
        canonical_label_pair = tuple(sorted((outcome_a_label, outcome_b_label)))
        prior_side_a_label_count, prior_side_a_label_win_rate = _prior_stat_summary(
            prior_side_label_stats,
            outcome_a_label,
        )
        prior_side_b_label_count, prior_side_b_label_win_rate = _prior_stat_summary(
            prior_side_label_stats,
            outcome_b_label,
        )
        prior_side_a_pair_count, prior_side_a_pair_win_rate = _prior_stat_summary(
            prior_label_pair_side_stats,
            (canonical_label_pair, outcome_a_label),
        )
        prior_side_b_pair_count, prior_side_b_pair_win_rate = _prior_stat_summary(
            prior_label_pair_side_stats,
            (canonical_label_pair, outcome_b_label),
        )
        prior_side_a_pattern_count, prior_side_a_pattern_win_rate = _prior_stat_summary(
            prior_question_pattern_side_stats,
            (question_pattern, outcome_a_label),
        )
        prior_side_b_pattern_count, prior_side_b_pattern_win_rate = _prior_stat_summary(
            prior_question_pattern_side_stats,
            (question_pattern, outcome_b_label),
        )
        prior_outcome_a_position_count, prior_outcome_a_position_win_rate = _prior_stat_summary(
            prior_side_position_stats,
            1,
        )
        prior_outcome_b_position_count, prior_outcome_b_position_win_rate = _prior_stat_summary(
            prior_side_position_stats,
            0,
        )
        prior_side_a_category_count, prior_side_a_category_win_rate = _prior_stat_summary(
            prior_event_category_side_stats,
            (event_category_key, outcome_a_label),
        )
        prior_side_b_category_count, prior_side_b_category_win_rate = _prior_stat_summary(
            prior_event_category_side_stats,
            (event_category_key, outcome_b_label),
        )
        prior_side_a_opening_count, prior_side_a_opening_win_rate = _prior_stat_summary(
            prior_question_opening_side_stats,
            (question_opening, outcome_a_label),
        )
        prior_side_b_opening_count, prior_side_b_opening_win_rate = _prior_stat_summary(
            prior_question_opening_side_stats,
            (question_opening, outcome_b_label),
        )
        prior_side_a_category_pattern_count, prior_side_a_category_pattern_win_rate = _prior_stat_summary(
            prior_category_pattern_side_stats,
            ((event_category_key, question_pattern), outcome_a_label),
        )
        prior_side_b_category_pattern_count, prior_side_b_category_pattern_win_rate = _prior_stat_summary(
            prior_category_pattern_side_stats,
            ((event_category_key, question_pattern), outcome_b_label),
        )

        for horizon in horizons:
            observation_time = market_end_time - timedelta(hours=horizon)
            eligible_transactions = [row for row in condition_transactions if row["transaction_time"] <= observation_time]
            historical_whale_snapshot = historical_whale_sets.get(
                observation_time,
                HistoricalWhaleSnapshot(
                    whale_user_ids=set(),
                    trusted_whale_user_ids=set(),
                    weighted_score_by_user={},
                    trades_per_active_day_by_user={},
                ),
            )
            whale_user_ids = historical_whale_snapshot.whale_user_ids
            trusted_whale_user_ids = historical_whale_snapshot.trusted_whale_user_ids
            weighted_score_by_user = historical_whale_snapshot.weighted_score_by_user
            trades_per_active_day_by_user = historical_whale_snapshot.trades_per_active_day_by_user

            outcomes = {
                outcome_a_label: OutcomeState(),
                outcome_b_label: OutcomeState(),
            }
            distinct_users: set[int] = set()
            buy_trade_count_total = 0
            sell_trade_count_total = 0
            total_notional = 0.0
            total_buy_notional = 0.0
            total_sell_notional = 0.0
            top_user_trade_count: dict[int, int] = {}
            top_user_notional: dict[int, float] = {}
            last_trade_at: datetime | None = None
            whale_distinct_users: set[int] = set()
            trusted_whale_distinct_users: set[int] = set()
            whale_trade_count_total = 0
            trusted_whale_trade_count_total = 0
            whale_notional_total = 0.0
            trusted_whale_notional_total = 0.0
            whale_buy_trade_count_total = 0
            whale_sell_trade_count_total = 0
            trusted_whale_buy_trade_count_total = 0
            trusted_whale_sell_trade_count_total = 0
            whale_weighted_score_total_by_user: dict[int, float] = {}
            whale_weighted_buy_pressure_total = 0.0
            whale_weighted_sell_pressure_total = 0.0
            trusted_whale_weighted_score_total_by_user: dict[int, float] = {}
            trusted_whale_weighted_buy_pressure_total = 0.0
            trusted_whale_weighted_sell_pressure_total = 0.0

            for trade in eligible_transactions:
                outcome_label = _normalized_label(str(trade["outcome_label"]))
                if outcome_label not in outcomes:
                    continue
                state = outcomes[outcome_label]
                state.trade_count += 1
                user_id = int(trade["user_id"])
                is_whale = user_id in whale_user_ids
                is_trusted_whale = user_id in trusted_whale_user_ids
                weighted_score = weighted_score_by_user.get(user_id, 0.0)
                state.distinct_users.add(user_id)
                distinct_users.add(user_id)
                top_user_trade_count[user_id] = top_user_trade_count.get(user_id, 0) + 1

                notional_value = float(trade["notional_value"] or 0.0)
                shares = float(trade["shares"] or 0.0)
                price = float(trade["price"] or 0.0)
                trade_time = trade["transaction_time"]

                state.total_notional += notional_value
                state.total_shares += shares
                state.price_sum += price
                state.last_price = price
                state.last_trade_at = trade_time
                if is_whale:
                    state.whale_trade_count += 1
                    state.whale_notional += notional_value
                    state.whale_weighted_score_sum += weighted_score
                    whale_weighted_score_total_by_user[user_id] = weighted_score
                    state.whale_user_notional[user_id] = state.whale_user_notional.get(user_id, 0.0) + notional_value
                    if state.first_whale_trade_at is None or trade_time < state.first_whale_trade_at:
                        state.first_whale_trade_at = trade_time
                    if state.last_whale_trade_at is None or trade_time > state.last_whale_trade_at:
                        state.last_whale_trade_at = trade_time
                    whale_distinct_users.add(user_id)
                    whale_trade_count_total += 1
                    whale_notional_total += notional_value
                if is_trusted_whale:
                    state.trusted_whale_trade_count += 1
                    state.trusted_whale_notional += notional_value
                    state.trusted_whale_user_ids.add(user_id)
                    state.trusted_whale_weighted_score_sum += weighted_score
                    trusted_whale_weighted_score_total_by_user[user_id] = weighted_score
                    state.trusted_whale_user_notional[user_id] = (
                        state.trusted_whale_user_notional.get(user_id, 0.0) + notional_value
                    )
                    if state.first_trusted_whale_trade_at is None or trade_time < state.first_trusted_whale_trade_at:
                        state.first_trusted_whale_trade_at = trade_time
                    if state.last_trusted_whale_trade_at is None or trade_time > state.last_trusted_whale_trade_at:
                        state.last_trusted_whale_trade_at = trade_time
                    trusted_whale_distinct_users.add(user_id)
                    trusted_whale_trade_count_total += 1
                    trusted_whale_notional_total += notional_value
                if state.min_price is None or price < state.min_price:
                    state.min_price = price
                if state.max_price is None or price > state.max_price:
                    state.max_price = price

                if trade["side"] == "buy":
                    buy_trade_count_total += 1
                    state.buy_notional += notional_value
                    if is_whale:
                        state.whale_buy_trade_count += 1
                        state.whale_buy_notional += notional_value
                        state.whale_weighted_buy_pressure += weighted_score * notional_value
                        whale_buy_trade_count_total += 1
                        whale_weighted_buy_pressure_total += weighted_score * notional_value
                    if is_trusted_whale:
                        state.trusted_whale_buy_trade_count += 1
                        state.trusted_whale_buy_notional += notional_value
                        state.trusted_whale_weighted_buy_pressure += (
                            weighted_score_by_user.get(user_id, 0.0) * notional_value
                        )
                        trusted_whale_buy_trade_count_total += 1
                        trusted_whale_weighted_buy_pressure_total += (
                            weighted_score_by_user.get(user_id, 0.0) * notional_value
                        )
                else:
                    sell_trade_count_total += 1
                    state.sell_notional += notional_value
                    if is_whale:
                        state.whale_sell_trade_count += 1
                        state.whale_sell_notional += notional_value
                        state.whale_weighted_sell_pressure += weighted_score * notional_value
                        whale_sell_trade_count_total += 1
                        whale_weighted_sell_pressure_total += weighted_score * notional_value
                    if is_trusted_whale:
                        state.trusted_whale_sell_trade_count += 1
                        state.trusted_whale_sell_notional += notional_value
                        state.trusted_whale_weighted_sell_pressure += (
                            weighted_score_by_user.get(user_id, 0.0) * notional_value
                        )
                        trusted_whale_sell_trade_count_total += 1
                        trusted_whale_weighted_sell_pressure_total += (
                            weighted_score_by_user.get(user_id, 0.0) * notional_value
                        )

                total_notional += notional_value
                top_user_notional[user_id] = top_user_notional.get(user_id, 0.0) + notional_value
                if trade["side"] == "buy":
                    total_buy_notional += notional_value
                else:
                    total_sell_notional += notional_value

                if last_trade_at is None or trade_time > last_trade_at:
                    last_trade_at = trade_time

            outcome_a = outcomes[outcome_a_label]
            outcome_b = outcomes[outcome_b_label]
            total_trade_count = outcome_a.trade_count + outcome_b.trade_count
            market_age_hours = _hours_between(observation_time, market_start_time)
            hours_to_close = _hours_between(market_end_time, observation_time)
            if total_trade_count == 0:
                included_sparse_counts["no_trades_before_cutoff"] += 1
            elif outcome_a.trade_count == 0 or outcome_b.trade_count == 0:
                included_sparse_counts["single_outcome_before_cutoff"] += 1
            trade_density_per_day = _safe_divide(total_trade_count, max(market_age_hours / 24.0, 1e-9))
            top_trade_share = _safe_divide(max(top_user_trade_count.values(), default=0), total_trade_count)
            top_notional_share = _safe_divide(max(top_user_notional.values(), default=0.0), total_notional)

            def make_row(
                side_label: str,
                side_state: OutcomeState,
                opposite_label: str,
                opposite_state: OutcomeState,
                *,
                side_is_outcome_a: bool,
            ) -> dict[str, Any]:
                side_tokens = _tokenize_text(side_label)
                opposite_tokens = _tokenize_text(opposite_label)
                side_position_feature = int(side_is_outcome_a)
                side_label_key = outcome_a_label if side_is_outcome_a else outcome_b_label
                if side_is_outcome_a:
                    prior_side_label_count = prior_side_a_label_count
                    prior_side_label_win_rate = prior_side_a_label_win_rate
                    prior_label_pair_count = prior_side_a_pair_count
                    prior_label_pair_side_win_rate = prior_side_a_pair_win_rate
                    prior_question_pattern_count = prior_side_a_pattern_count
                    prior_question_pattern_side_win_rate = prior_side_a_pattern_win_rate
                    prior_side_position_count = prior_outcome_a_position_count
                    prior_side_position_win_rate = prior_outcome_a_position_win_rate
                    prior_event_category_count = prior_side_a_category_count
                    prior_event_category_side_win_rate = prior_side_a_category_win_rate
                    prior_question_opening_count = prior_side_a_opening_count
                    prior_question_opening_side_win_rate = prior_side_a_opening_win_rate
                    prior_category_pattern_count = prior_side_a_category_pattern_count
                    prior_category_pattern_side_win_rate = prior_side_a_category_pattern_win_rate
                else:
                    prior_side_label_count = prior_side_b_label_count
                    prior_side_label_win_rate = prior_side_b_label_win_rate
                    prior_label_pair_count = prior_side_b_pair_count
                    prior_label_pair_side_win_rate = prior_side_b_pair_win_rate
                    prior_question_pattern_count = prior_side_b_pattern_count
                    prior_question_pattern_side_win_rate = prior_side_b_pattern_win_rate
                    prior_side_position_count = prior_outcome_b_position_count
                    prior_side_position_win_rate = prior_outcome_b_position_win_rate
                    prior_event_category_count = prior_side_b_category_count
                    prior_event_category_side_win_rate = prior_side_b_category_win_rate
                    prior_question_opening_count = prior_side_b_opening_count
                    prior_question_opening_side_win_rate = prior_side_b_opening_win_rate
                    prior_category_pattern_count = prior_side_b_category_pattern_count
                    prior_category_pattern_side_win_rate = prior_side_b_category_pattern_win_rate
                side_label_wins = int(winning_outcome == side_label)
                avg_price_side = _average_price(side_state)
                avg_price_opposite = _average_price(opposite_state)
                side_last_price = _observed_price(side_state, fallback=avg_price_side)
                opposite_last_price = _observed_price(opposite_state, fallback=avg_price_opposite)
                price_baseline = side_last_price if side_state.last_price is not None else avg_price_side
                resolution_edge = round(side_label_wins - price_baseline, 8)
                side_whale_net_notional = side_state.whale_buy_notional - side_state.whale_sell_notional
                side_whale_weighted_net_pressure = (
                    side_state.whale_weighted_buy_pressure
                    - side_state.whale_weighted_sell_pressure
                )
                side_trusted_whale_net_notional = (
                    side_state.trusted_whale_buy_notional - side_state.trusted_whale_sell_notional
                )
                side_trusted_whale_weighted_net_pressure = (
                    side_state.trusted_whale_weighted_buy_pressure
                    - side_state.trusted_whale_weighted_sell_pressure
                )
                total_trusted_whale_weighted_net_pressure = (
                    trusted_whale_weighted_buy_pressure_total - trusted_whale_weighted_sell_pressure_total
                )
                total_whale_weighted_net_pressure = whale_weighted_buy_pressure_total - whale_weighted_sell_pressure_total
                side_whale_count = len(side_state.whale_user_notional)
                side_trusted_whale_count = len(side_state.trusted_whale_user_ids)
                total_whale_count = len(whale_distinct_users)
                total_trusted_whale_count = len(trusted_whale_distinct_users)
                side_crowd_notional = max(side_state.total_notional - side_state.whale_notional, 0.0)
                side_crowd_net_notional = (
                    (side_state.buy_notional - side_state.whale_buy_notional)
                    - (side_state.sell_notional - side_state.whale_sell_notional)
                )
                side_trusted_crowd_notional = max(side_state.total_notional - side_state.trusted_whale_notional, 0.0)
                side_trusted_crowd_net_notional = (
                    (side_state.buy_notional - side_state.trusted_whale_buy_notional)
                    - (side_state.sell_notional - side_state.trusted_whale_sell_notional)
                )
                whale_side_net_notional_share = _safe_divide(side_whale_net_notional, side_state.total_notional)
                trusted_whale_side_net_notional_share = _safe_divide(
                    side_trusted_whale_net_notional,
                    side_state.total_notional,
                )
                crowd_side_net_notional_share = _safe_divide(side_crowd_net_notional, side_crowd_notional)
                trusted_crowd_side_net_notional_share = _safe_divide(
                    side_trusted_crowd_net_notional,
                    side_trusted_crowd_notional,
                )
                trusted_whale_side_behavior = _summarize_position_behavior(
                    eligible_transactions,
                    user_ids=trusted_whale_user_ids,
                    outcome_label=side_label,
                    observation_time=observation_time,
                )
                trusted_whale_side_recent_pressure = _recent_trusted_whale_pressure_features(
                    eligible_transactions,
                    trusted_whale_user_ids=trusted_whale_user_ids,
                    weighted_score_by_user=weighted_score_by_user,
                    outcome_label=side_label,
                    observation_time=observation_time,
                    side_total_notional=side_state.total_notional,
                )
                trusted_whale_side_avg_trades_per_active_day = _safe_average(
                    [
                        trades_per_active_day_by_user.get(user_id, 0.0)
                        for user_id in side_state.trusted_whale_user_ids
                    ]
                )
                future_targets: dict[str, float] = {}
                for window_hours in FUTURE_MOVEMENT_WINDOWS_HOURS:
                    future_targets.update(
                        _future_price_summary(
                            condition_transactions=condition_transactions,
                            side_label=side_label,
                            observation_time=observation_time,
                            market_end_time=market_end_time,
                            side_last_price=side_last_price,
                            side_label_wins=side_label_wins,
                            window_hours=window_hours,
                        )
                    )
                return {
                    "dataset_version": DATASET_VERSION,
                    "condition_ref": condition_ref,
                    "event_id": int(meta["event_id"]),
                    "event_title": str(meta["event_title"]),
                    "event_category": event_category_text,
                    "event_slug": meta["event_slug"] or "",
                    "market_contract_id": int(meta["market_contract_id"]),
                    "market_slug": meta["market_slug"] or "",
                    "question": str(meta["question"]),
                    "outcome_a_label": outcome_a_label,
                    "outcome_b_label": outcome_b_label,
                    "side_label": side_label,
                    "opposite_side_label": opposite_label,
                    "side_is_outcome_a": int(side_is_outcome_a),
                    "observation_time": _iso(observation_time),
                    "market_start_time": _iso(market_start_time),
                    "market_end_time": _iso(market_end_time),
                    "horizon_hours": horizon,
                    "hours_to_close": hours_to_close,
                    "market_age_hours": market_age_hours,
                    "market_duration_hours": market_duration_hours,
                    "side_position_feature": side_position_feature,
                    "question_char_length": question_char_length,
                    "question_token_count": question_token_count,
                    "event_title_char_length": event_title_char_length,
                    "event_title_token_count": event_title_token_count,
                    "event_description_char_length": event_description_char_length,
                    "event_description_token_count": event_description_token_count,
                    "event_category_token_count": event_category_token_count,
                    "event_tag_count": event_tag_count,
                    "event_tag_token_count": event_tag_token_count,
                    "market_slug_token_count": market_slug_token_count,
                    "event_slug_token_count": event_slug_token_count,
                    "question_title_token_overlap": question_title_token_overlap,
                    "question_description_token_overlap": question_description_token_overlap,
                    "question_category_token_overlap": question_category_token_overlap,
                    "question_tag_token_overlap": question_tag_token_overlap,
                    "question_side_token_overlap": _token_overlap_ratio(question_tokens, side_tokens),
                    "question_opposite_token_overlap": _token_overlap_ratio(question_tokens, opposite_tokens),
                    "side_category_token_overlap": _token_overlap_ratio(side_tokens, event_category_tokens),
                    "opposite_side_category_token_overlap": _token_overlap_ratio(opposite_tokens, event_category_tokens),
                    "side_tag_token_overlap": _token_overlap_ratio(side_tokens, event_tag_tokens),
                    "opposite_side_tag_token_overlap": _token_overlap_ratio(opposite_tokens, event_tag_tokens),
                    "side_label_char_length": len(side_label),
                    "side_label_token_count": len(side_tokens),
                    "opposite_side_label_char_length": len(opposite_label),
                    "opposite_side_label_token_count": len(opposite_tokens),
                    "side_label_is_yes": int(side_label_key == "yes"),
                    "side_label_is_no": int(side_label_key == "no"),
                    "opposite_label_is_yes": int(opposite_label == "yes"),
                    "opposite_label_is_no": int(opposite_label == "no"),
                    "question_has_number": question_has_number,
                    "question_digit_count": question_digit_count,
                    "question_has_percent": question_has_percent,
                    "question_has_dollar": question_has_dollar,
                    "question_has_more_than": question_has_more_than,
                    "question_has_less_than": question_has_less_than,
                    "question_has_date_reference": question_has_date_reference,
                    "question_starts_with_who_or_which": question_starts_with_who_or_which,
                    "question_title_exact_match": question_title_exact_match,
                    "market_volume_log1p": _safe_log1p(market_volume),
                    "market_liquidity_log1p": _safe_log1p(market_liquidity),
                    "prior_side_label_count": prior_side_label_count,
                    "prior_side_label_win_rate": prior_side_label_win_rate,
                    "prior_label_pair_count": prior_label_pair_count,
                    "prior_label_pair_side_win_rate": prior_label_pair_side_win_rate,
                    "prior_question_pattern_count": prior_question_pattern_count,
                    "prior_question_pattern_side_win_rate": prior_question_pattern_side_win_rate,
                    "prior_side_position_count": prior_side_position_count,
                    "prior_side_position_win_rate": prior_side_position_win_rate,
                    "prior_event_category_count": prior_event_category_count,
                    "prior_event_category_side_win_rate": prior_event_category_side_win_rate,
                    "prior_question_opening_count": prior_question_opening_count,
                    "prior_question_opening_side_win_rate": prior_question_opening_side_win_rate,
                    "prior_category_pattern_count": prior_category_pattern_count,
                    "prior_category_pattern_side_win_rate": prior_category_pattern_side_win_rate,
                    "has_any_trade_before_cutoff": int(total_trade_count > 0),
                    "has_both_outcomes_before_cutoff": int(outcome_a.trade_count > 0 and outcome_b.trade_count > 0),
                    "side_has_trade_before_cutoff": int(side_state.trade_count > 0),
                    "opposite_has_trade_before_cutoff": int(opposite_state.trade_count > 0),
                    "side_price_observed": int(side_state.last_price is not None),
                    "opposite_price_observed": int(opposite_state.last_price is not None),
                    "trade_count_total": total_trade_count,
                    "trade_count_outcome_a": outcome_a.trade_count,
                    "trade_count_outcome_b": outcome_b.trade_count,
                    "buy_trade_count_total": buy_trade_count_total,
                    "sell_trade_count_total": sell_trade_count_total,
                    "distinct_users": len(distinct_users),
                    "top_user_trade_share": top_trade_share,
                    "top_user_notional_share": top_notional_share,
                    "total_notional": round(total_notional, 8),
                    "total_buy_notional": round(total_buy_notional, 8),
                    "total_sell_notional": round(total_sell_notional, 8),
                    "whale_distinct_users": len(whale_distinct_users),
                    "trusted_whale_distinct_users": len(trusted_whale_distinct_users),
                    "whale_trade_share": _safe_divide(whale_trade_count_total, total_trade_count),
                    "trusted_whale_trade_share": _safe_divide(trusted_whale_trade_count_total, total_trade_count),
                    "whale_notional_share": _safe_divide(whale_notional_total, total_notional),
                    "trusted_whale_notional_share": _safe_divide(trusted_whale_notional_total, total_notional),
                    "whale_buy_trade_count_total": whale_buy_trade_count_total,
                    "whale_sell_trade_count_total": whale_sell_trade_count_total,
                    "whale_weighted_score_sum_total": round(
                        sum(whale_weighted_score_total_by_user.values()),
                        8,
                    ),
                    "whale_weighted_net_pressure_total": round(total_whale_weighted_net_pressure, 8),
                    "whale_weighted_net_pressure_total_per_total_notional": _safe_divide(
                        total_whale_weighted_net_pressure,
                        total_notional,
                    ),
                    "whale_weighted_net_pressure_total_per_market_liquidity": _safe_divide(
                        total_whale_weighted_net_pressure,
                        market_liquidity,
                    ),
                    "whale_weighted_net_pressure_total_per_whale": _safe_divide(
                        total_whale_weighted_net_pressure,
                        total_whale_count,
                    ),
                    "trusted_whale_buy_trade_count_total": trusted_whale_buy_trade_count_total,
                    "trusted_whale_sell_trade_count_total": trusted_whale_sell_trade_count_total,
                    "trusted_whale_buy_sell_ratio_total": _safe_divide(
                        trusted_whale_buy_trade_count_total,
                        max(trusted_whale_sell_trade_count_total, 1),
                    ),
                    "trusted_whale_weighted_score_sum_total": round(
                        sum(trusted_whale_weighted_score_total_by_user.values()),
                        8,
                    ),
                    "trusted_whale_weighted_net_pressure_total": round(
                        total_trusted_whale_weighted_net_pressure,
                        8,
                    ),
                    "trusted_whale_weighted_net_pressure_total_per_total_notional": _safe_divide(
                        total_trusted_whale_weighted_net_pressure,
                        total_notional,
                    ),
                    "trusted_whale_weighted_net_pressure_total_per_market_liquidity": _safe_divide(
                        total_trusted_whale_weighted_net_pressure,
                        market_liquidity,
                    ),
                    "trusted_whale_weighted_net_pressure_total_per_trusted_whale": _safe_divide(
                        total_trusted_whale_weighted_net_pressure,
                        total_trusted_whale_count,
                    ),
                    "side_trade_count": side_state.trade_count,
                    "opposite_trade_count": opposite_state.trade_count,
                    "side_distinct_users": len(side_state.distinct_users),
                    "opposite_distinct_users": len(opposite_state.distinct_users),
                    "side_buy_notional": round(side_state.buy_notional, 8),
                    "side_sell_notional": round(side_state.sell_notional, 8),
                    "opposite_buy_notional": round(opposite_state.buy_notional, 8),
                    "opposite_sell_notional": round(opposite_state.sell_notional, 8),
                    "side_net_notional": round(side_state.buy_notional - side_state.sell_notional, 8),
                    "opposite_net_notional": round(opposite_state.buy_notional - opposite_state.sell_notional, 8),
                    "side_total_shares": round(side_state.total_shares, 8),
                    "opposite_total_shares": round(opposite_state.total_shares, 8),
                    "last_price_side": side_last_price,
                    "last_price_opposite": opposite_last_price,
                    "avg_price_side": avg_price_side,
                    "avg_price_opposite": avg_price_opposite,
                    "min_price_side": _observed_extreme(side_state.min_price, fallback=avg_price_side),
                    "min_price_opposite": _observed_extreme(opposite_state.min_price, fallback=avg_price_opposite),
                    "max_price_side": _observed_extreme(side_state.max_price, fallback=avg_price_side),
                    "max_price_opposite": _observed_extreme(opposite_state.max_price, fallback=avg_price_opposite),
                    "side_trade_share": _safe_divide(side_state.trade_count, total_trade_count),
                    "side_buy_notional_share": _safe_divide(side_state.buy_notional, total_buy_notional),
                    "whale_side_trade_share": _safe_divide(side_state.whale_trade_count, side_state.trade_count),
                    "trusted_whale_side_trade_share": _safe_divide(
                        side_state.trusted_whale_trade_count,
                        side_state.trade_count,
                    ),
                    "whale_side_notional_share": _safe_divide(side_state.whale_notional, side_state.total_notional),
                    "trusted_whale_side_notional_share": _safe_divide(
                        side_state.trusted_whale_notional,
                        side_state.total_notional,
                    ),
                    "whale_side_buy_notional_share": _safe_divide(side_state.whale_buy_notional, side_state.buy_notional),
                    "whale_side_sell_notional_share": _safe_divide(side_state.whale_sell_notional, side_state.sell_notional),
                    "trusted_whale_side_buy_notional_share": _safe_divide(
                        side_state.trusted_whale_buy_notional,
                        side_state.buy_notional,
                    ),
                    "trusted_whale_side_sell_notional_share": _safe_divide(
                        side_state.trusted_whale_sell_notional,
                        side_state.sell_notional,
                    ),
                    "whale_side_net_notional_share": whale_side_net_notional_share,
                    "whale_side_weighted_score_sum": round(side_state.whale_weighted_score_sum, 8),
                    "whale_side_weighted_buy_pressure": round(side_state.whale_weighted_buy_pressure, 8),
                    "whale_side_weighted_sell_pressure": round(side_state.whale_weighted_sell_pressure, 8),
                    "whale_side_weighted_net_pressure": round(side_whale_weighted_net_pressure, 8),
                    "whale_side_weighted_net_pressure_per_side_notional": _safe_divide(
                        side_whale_weighted_net_pressure,
                        side_state.total_notional,
                    ),
                    "whale_side_weighted_net_pressure_per_total_notional": _safe_divide(
                        side_whale_weighted_net_pressure,
                        total_notional,
                    ),
                    "whale_side_weighted_net_pressure_per_market_liquidity": _safe_divide(
                        side_whale_weighted_net_pressure,
                        market_liquidity,
                    ),
                    "whale_side_weighted_net_pressure_per_whale": _safe_divide(
                        side_whale_weighted_net_pressure,
                        side_whale_count,
                    ),
                    "trusted_whale_side_net_notional_share": trusted_whale_side_net_notional_share,
                    "trusted_whale_side_buy_trade_count": side_state.trusted_whale_buy_trade_count,
                    "trusted_whale_side_sell_trade_count": side_state.trusted_whale_sell_trade_count,
                    "trusted_whale_side_buy_sell_ratio": _safe_divide(
                        side_state.trusted_whale_buy_trade_count,
                        max(side_state.trusted_whale_sell_trade_count, 1),
                    ),
                    "trusted_whale_side_weighted_score_sum": round(side_state.trusted_whale_weighted_score_sum, 8),
                    "trusted_whale_side_weighted_buy_pressure": round(
                        side_state.trusted_whale_weighted_buy_pressure,
                        8,
                    ),
                    "trusted_whale_side_weighted_sell_pressure": round(
                        side_state.trusted_whale_weighted_sell_pressure,
                        8,
                    ),
                    "trusted_whale_side_weighted_net_pressure": round(side_trusted_whale_weighted_net_pressure, 8),
                    "trusted_whale_side_weighted_net_pressure_per_side_notional": _safe_divide(
                        side_trusted_whale_weighted_net_pressure,
                        side_state.total_notional,
                    ),
                    "trusted_whale_side_weighted_net_pressure_per_total_notional": _safe_divide(
                        side_trusted_whale_weighted_net_pressure,
                        total_notional,
                    ),
                    "trusted_whale_side_weighted_net_pressure_per_market_liquidity": _safe_divide(
                        side_trusted_whale_weighted_net_pressure,
                        market_liquidity,
                    ),
                    "trusted_whale_side_weighted_net_pressure_per_trusted_whale": _safe_divide(
                        side_trusted_whale_weighted_net_pressure,
                        side_trusted_whale_count,
                    ),
                    "trusted_whale_side_entry_exit_gap": round(
                        side_state.trusted_whale_weighted_buy_pressure
                        - side_state.trusted_whale_weighted_sell_pressure,
                        8,
                    ),
                    "trusted_whale_side_avg_trades_per_active_day": trusted_whale_side_avg_trades_per_active_day,
                    "trusted_whale_side_entry_trade_count": trusted_whale_side_behavior.entry_trade_count,
                    "trusted_whale_side_exit_trade_count": trusted_whale_side_behavior.exit_trade_count,
                    "trusted_whale_side_partial_exit_count": trusted_whale_side_behavior.partial_exit_count,
                    "trusted_whale_side_full_exit_count": trusted_whale_side_behavior.full_exit_count,
                    "trusted_whale_side_unmatched_sell_count": trusted_whale_side_behavior.unmatched_sell_count,
                    "trusted_whale_side_avg_holding_hours": trusted_whale_side_behavior.avg_holding_hours,
                    "trusted_whale_side_avg_open_holding_hours": trusted_whale_side_behavior.avg_open_holding_hours,
                    "trusted_whale_side_realized_pnl": trusted_whale_side_behavior.realized_pnl,
                    "trusted_whale_side_realized_roi": trusted_whale_side_behavior.realized_roi,
                    "trusted_whale_side_avg_exit_profit": trusted_whale_side_behavior.avg_exit_profit,
                    **trusted_whale_side_recent_pressure,
                    "whale_vs_crowd_side_net_notional_gap": round(
                        whale_side_net_notional_share - crowd_side_net_notional_share,
                        8,
                    ),
                    "trusted_whale_vs_crowd_side_net_notional_gap": round(
                        trusted_whale_side_net_notional_share - trusted_crowd_side_net_notional_share,
                        8,
                    ),
                    "top_whale_side_notional_share": _safe_divide(
                        max(side_state.whale_user_notional.values(), default=0.0),
                        side_state.total_notional,
                    ),
                    "top_trusted_whale_side_notional_share": _safe_divide(
                        max(side_state.trusted_whale_user_notional.values(), default=0.0),
                        side_state.total_notional,
                    ),
                    "first_whale_trade_age_side_hours": _last_trade_age_hours(
                        observation_time,
                        side_state.first_whale_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "last_whale_trade_age_side_hours": _last_trade_age_hours(
                        observation_time,
                        side_state.last_whale_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "first_trusted_whale_trade_age_side_hours": _last_trade_age_hours(
                        observation_time,
                        side_state.first_trusted_whale_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "last_trusted_whale_trade_age_side_hours": _last_trade_age_hours(
                        observation_time,
                        side_state.last_trusted_whale_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "price_gap_side_minus_opposite": round(side_last_price - opposite_last_price, 8),
                    "price_abs_distance_from_even": round(abs(side_last_price - 0.5), 8),
                    "last_trade_age_hours": _last_trade_age_hours(
                        observation_time,
                        last_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "last_trade_age_side_hours": _last_trade_age_hours(
                        observation_time,
                        side_state.last_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "last_trade_age_opposite_hours": _last_trade_age_hours(
                        observation_time,
                        opposite_state.last_trade_at,
                        market_age_hours=market_age_hours,
                    ),
                    "trade_density_per_day": trade_density_per_day,
                    PRICE_BASELINE_COLUMN: round(price_baseline, 8),
                    RESOLUTION_EDGE_COLUMN: resolution_edge,
                    **future_targets,
                    "winning_outcome_label": winning_outcome,
                    "resolution_source": resolution_detail.resolution_source if resolution_detail else "unknown",
                    "resolution_confidence": resolution_detail.resolution_confidence if resolution_detail else 0.0,
                    "resolution_time": _iso(resolution_detail.resolution_time) if resolution_detail else "",
                    TARGET_COLUMN: side_label_wins,
                }

            dataset_rows.append(
                make_row(
                    outcome_a_label,
                    outcome_a,
                    outcome_b_label,
                    outcome_b,
                    side_is_outcome_a=True,
                )
            )
            dataset_rows.append(
                make_row(
                    outcome_b_label,
                    outcome_b,
                    outcome_a_label,
                    outcome_a,
                    side_is_outcome_a=False,
                )
            )
            horizon_counts[str(horizon)] += 1

        _update_prior_stat(
            prior_side_label_stats,
            outcome_a_label,
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_side_label_stats,
            outcome_b_label,
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_label_pair_side_stats,
            (canonical_label_pair, outcome_a_label),
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_label_pair_side_stats,
            (canonical_label_pair, outcome_b_label),
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_question_pattern_side_stats,
            (question_pattern, outcome_a_label),
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_question_pattern_side_stats,
            (question_pattern, outcome_b_label),
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_side_position_stats,
            1,
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_side_position_stats,
            0,
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_event_category_side_stats,
            (event_category_key, outcome_a_label),
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_event_category_side_stats,
            (event_category_key, outcome_b_label),
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_question_opening_side_stats,
            (question_opening, outcome_a_label),
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_question_opening_side_stats,
            (question_opening, outcome_b_label),
            did_win=int(winning_outcome == outcome_b_label),
        )
        _update_prior_stat(
            prior_category_pattern_side_stats,
            ((event_category_key, question_pattern), outcome_a_label),
            did_win=int(winning_outcome == outcome_a_label),
        )
        _update_prior_stat(
            prior_category_pattern_side_stats,
            ((event_category_key, question_pattern), outcome_b_label),
            did_win=int(winning_outcome == outcome_b_label),
        )

    metadata = {
        "dataset_version": DATASET_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_column": TARGET_COLUMN,
        "group_key_column": GROUP_KEY_COLUMN,
        "row_count": len(dataset_rows),
        "feature_columns": list(FEATURE_COLUMNS),
        "analysis_columns": list(ANALYSIS_COLUMNS),
        "identifier_columns": list(IDENTIFIER_COLUMNS),
        "audit_columns": list(AUDIT_COLUMNS),
        "horizon_hours": list(horizons),
        "future_movement_windows_hours": list(FUTURE_MOVEMENT_WINDOWS_HOURS),
        "recent_whale_windows_hours": list(RECENT_WHALE_WINDOWS_HOURS),
        "scored_whale_pressure_columns": list(SCORED_WHALE_PRESSURE_FEATURE_COLUMNS),
        "normalized_trusted_whale_pressure_columns": list(NORMALIZED_TRUSTED_WHALE_PRESSURE_FEATURE_COLUMNS),
        "whale_weight_config": whale_weight_config.as_dict(),
        "horizon_row_counts": horizon_counts,
        "class_balance": {
            "side_wins": sum(int(row[TARGET_COLUMN]) for row in dataset_rows),
            "side_loses": len(dataset_rows) - sum(int(row[TARGET_COLUMN]) for row in dataset_rows),
        },
        "leakage_audit": _dataset_leakage_audit(dataset_rows),
        "excluded_counts": excluded_counts,
        "included_sparse_counts": included_sparse_counts,
        "assumptions": [
            "Primary ML target is market-level outcome prediction, not user profitability.",
            "Rows represent pre-close market snapshots for resolved Polymarket condition sides.",
            "Observation rows are generated at fixed hours-before-close horizons.",
            "Only trades at or before the observation cutoff are included.",
            "Sparse early snapshots are retained even when one or both outcomes have no pre-cutoff trades.",
            "Cold-start rows include static question/label/slug/category/tag metadata plus prior features computed only from older resolved markets.",
            "Cold-start prior win-rate features are smoothed toward 0.5 so sparse buckets do not immediately collapse to 0 or 1.",
            "No orderbook features are included because resolved-market orderbook history is sparse in the current dataset.",
            "Outcome labels are normalized using the same conservative resolver as whale scoring.",
            "Closed-market resolution prefers official Polymarket Gamma outcomePrices, with price thresholds only as fallback.",
            "Whale participation features are computed from cumulative trade and resolved-market history available on or before each observation cutoff.",
            "Scored whale weighted-pressure features use the same versioned whale_weight_config for the broader whale cohort to improve trade-covered coverage.",
            "Trusted whale weighted-pressure features use the versioned whale_weight_config recorded in this metadata file.",
            "Trusted whale weighted-pressure features include raw net pressure and normalized variants scaled by notional, liquidity, and trusted-whale counts.",
            "Recent trusted-whale entry/exit pressure features summarize 1h, 6h, 12h, and 24h pre-cutoff activity with score-weighted and time-decayed pressure.",
            "Future movement targets report side-price deltas 12h and 24h after each observation cutoff.",
            "Trusted whale entry/exit behavior is reconstructed from matched buy/sell lots instead of treating every sell as a complete exit.",
            "Historical current exposure is approximated from open shares valued at average buy price because full point-in-time position marks are not available for every user.",
            "price_baseline uses the latest side price at the cutoff when available and otherwise falls back to the average side trade price.",
            "resolution_edge measures outcome residual relative to the cutoff-time price baseline and is intended for whale-signal analysis.",
            "Missing point-in-time prices fall back to a neutral 0.5 baseline and are paired with explicit coverage flags.",
        ],
    }
    return dataset_rows, metadata


def export_market_snapshot_dataset(
    session: Session,
    *,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    horizon_hours: tuple[int, ...] | list[int] | None = None,
    whale_weight_config_path: Path | None = None,
) -> dict[str, Any]:
    """Build and write the market-level ML dataset plus metadata."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    metadata_path = metadata_path or DEFAULT_METADATA_PATH
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    rows, metadata = build_market_snapshot_dataset(
        session,
        horizon_hours=horizon_hours,
        whale_weight_config_path=whale_weight_config_path,
    )
    with dataset_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)

    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return {
        "dataset_path": str(dataset_path),
        "metadata_path": str(metadata_path),
        "dataset_version": DATASET_VERSION,
        "row_count": metadata["row_count"],
        "class_balance": metadata["class_balance"],
        "horizon_row_counts": metadata["horizon_row_counts"],
        "excluded_counts": metadata["excluded_counts"],
    }
