"""Build the first market-level ML dataset from resolved Polymarket conditions."""

from __future__ import annotations

import csv
import json
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
    load_resolved_market_outcomes,
)


DATASET_VERSION = "ml_market_snapshot_v3"
DEFAULT_HORIZON_HOURS = (720, 168, 72, 24, 6, 1)
DEFAULT_OUTPUT_DIR = Path("data_platform/runtime/ml")
DEFAULT_DATASET_PATH = DEFAULT_OUTPUT_DIR / "resolved_market_snapshot_features.csv"
DEFAULT_METADATA_PATH = DEFAULT_OUTPUT_DIR / "resolved_market_snapshot_features.metadata.json"
TARGET_COLUMN = "label_side_wins"
GROUP_KEY_COLUMN = "condition_ref"

FEATURE_COLUMNS = (
    "horizon_hours",
    "hours_to_close",
    "market_age_hours",
    "market_duration_hours",
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
    "price_gap_side_minus_opposite",
    "price_abs_distance_from_even",
    "last_trade_age_hours",
    "last_trade_age_side_hours",
    "last_trade_age_opposite_hours",
    "trade_density_per_day",
)

WHALE_FEATURE_COLUMNS = (
    "whale_distinct_users",
    "trusted_whale_distinct_users",
    "whale_trade_share",
    "trusted_whale_trade_share",
    "whale_notional_share",
    "trusted_whale_notional_share",
    "whale_side_trade_share",
    "trusted_whale_side_trade_share",
    "whale_side_notional_share",
    "trusted_whale_side_notional_share",
)

IDENTIFIER_COLUMNS = (
    "dataset_version",
    "condition_ref",
    "event_id",
    "event_title",
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
    "label_side_wins",
)

CSV_COLUMNS = IDENTIFIER_COLUMNS + FEATURE_COLUMNS + AUDIT_COLUMNS


CONDITION_METADATA_SQL = text(
    """
    WITH ranked_conditions AS (
      SELECT
        mc.condition_ref,
        me.event_id,
        me.title AS event_title,
        me.slug AS event_slug,
        mc.market_contract_id,
        mc.market_slug,
        mc.question,
        mc.outcome_a_label,
        mc.outcome_b_label,
        COALESCE(mc.start_time, me.start_time) AS market_start_time,
        COALESCE(mc.end_time, me.end_time, me.closed_time) AS market_end_time,
        ROW_NUMBER() OVER (
          PARTITION BY mc.condition_ref
          ORDER BY COALESCE(mc.end_time, me.end_time, me.closed_time) DESC, mc.market_contract_id DESC
        ) AS rn
      FROM analytics.market_contract mc
      JOIN analytics.market_event me
        ON me.event_id = mc.event_id
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
      event_slug,
      market_contract_id,
      market_slug,
      question,
      outcome_a_label,
      outcome_b_label,
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
    whale_notional: float = 0.0
    trusted_whale_notional: float = 0.0

    def __post_init__(self) -> None:
        """Initialize mutable defaults safely."""
        if self.distinct_users is None:
            self.distinct_users = set()


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
) -> dict[datetime, tuple[set[int], set[int]]]:
    """Return whale and trusted-whale user sets for each observation cutoff."""
    user_lookup = _load_polymarket_user_lookup(session)
    metric_state_by_user: dict[int, HistoricalMetricState] = {}
    condition_state_by_user: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    condition_user_ids: dict[str, set[int]] = {}
    resolved_aggregate_by_user: dict[int, ResolvedAggregateState] = {}
    resolved_contribution_by_user_condition: dict[tuple[int, str], ResolvedContribution] = {}
    open_exposure_by_user: dict[int, float] = {}
    open_exposure_by_user_condition: dict[tuple[int, str], float] = {}
    historical_sets: dict[datetime, tuple[set[int], set[int]]] = {}
    previous_sets: tuple[set[int], set[int]] = (set(), set())
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
            historical_sets[observation_time] = previous_sets
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
        previous_sets = (whale_user_ids, trusted_whale_user_ids)
        historical_sets[observation_time] = previous_sets

    return historical_sets


def build_market_snapshot_dataset(
    session: Session,
    *,
    horizon_hours: tuple[int, ...] | list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build point-in-time market snapshots for resolved Polymarket conditions."""
    resolved_outcomes = load_resolved_market_outcomes(session)
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

    for condition_ref, meta in metadata_by_condition.items():
        winning_outcome = resolved_outcomes.get(condition_ref)
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

        for horizon in horizons:
            observation_time = market_end_time - timedelta(hours=horizon)
            eligible_transactions = [row for row in condition_transactions if row["transaction_time"] <= observation_time]
            if not eligible_transactions:
                excluded_counts["missing_transactions_before_cutoff"] += 1
                continue
            whale_user_ids, trusted_whale_user_ids = historical_whale_sets.get(observation_time, (set(), set()))

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

            for trade in eligible_transactions:
                outcome_label = _normalized_label(str(trade["outcome_label"]))
                if outcome_label not in outcomes:
                    continue
                state = outcomes[outcome_label]
                state.trade_count += 1
                user_id = int(trade["user_id"])
                is_whale = user_id in whale_user_ids
                is_trusted_whale = user_id in trusted_whale_user_ids
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
                    whale_distinct_users.add(user_id)
                    whale_trade_count_total += 1
                    whale_notional_total += notional_value
                if is_trusted_whale:
                    state.trusted_whale_trade_count += 1
                    state.trusted_whale_notional += notional_value
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
                else:
                    sell_trade_count_total += 1
                    state.sell_notional += notional_value

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
            if outcome_a.trade_count == 0 and outcome_b.trade_count == 0:
                excluded_counts["missing_transactions_before_cutoff"] += 1
                continue
            if outcome_a.trade_count == 0 or outcome_b.trade_count == 0:
                excluded_counts["missing_both_outcomes_before_cutoff"] += 1
                continue

            total_trade_count = outcome_a.trade_count + outcome_b.trade_count
            market_age_hours = _hours_between(observation_time, market_start_time)
            hours_to_close = _hours_between(market_end_time, observation_time)
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
                side_last_price = float(side_state.last_price or 0.0)
                opposite_last_price = float(opposite_state.last_price or 0.0)
                return {
                    "dataset_version": DATASET_VERSION,
                    "condition_ref": condition_ref,
                    "event_id": int(meta["event_id"]),
                    "event_title": str(meta["event_title"]),
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
                    "last_price_side": round(side_last_price, 8),
                    "last_price_opposite": round(opposite_last_price, 8),
                    "avg_price_side": _safe_divide(side_state.price_sum, side_state.trade_count),
                    "avg_price_opposite": _safe_divide(opposite_state.price_sum, opposite_state.trade_count),
                    "min_price_side": round(float(side_state.min_price or 0.0), 8),
                    "min_price_opposite": round(float(opposite_state.min_price or 0.0), 8),
                    "max_price_side": round(float(side_state.max_price or 0.0), 8),
                    "max_price_opposite": round(float(opposite_state.max_price or 0.0), 8),
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
                    "price_gap_side_minus_opposite": round(side_last_price - opposite_last_price, 8),
                    "price_abs_distance_from_even": round(abs(side_last_price - 0.5), 8),
                    "last_trade_age_hours": _hours_between(observation_time, last_trade_at),
                    "last_trade_age_side_hours": _hours_between(observation_time, side_state.last_trade_at),
                    "last_trade_age_opposite_hours": _hours_between(observation_time, opposite_state.last_trade_at),
                    "trade_density_per_day": trade_density_per_day,
                    "winning_outcome_label": winning_outcome,
                    TARGET_COLUMN: int(winning_outcome == side_label),
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

    metadata = {
        "dataset_version": DATASET_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_column": TARGET_COLUMN,
        "group_key_column": GROUP_KEY_COLUMN,
        "row_count": len(dataset_rows),
        "feature_columns": list(FEATURE_COLUMNS),
        "identifier_columns": list(IDENTIFIER_COLUMNS),
        "audit_columns": list(AUDIT_COLUMNS),
        "horizon_hours": list(horizons),
        "horizon_row_counts": horizon_counts,
        "class_balance": {
            "side_wins": sum(int(row[TARGET_COLUMN]) for row in dataset_rows),
            "side_loses": len(dataset_rows) - sum(int(row[TARGET_COLUMN]) for row in dataset_rows),
        },
        "excluded_counts": excluded_counts,
        "assumptions": [
            "Primary ML target is market-level outcome prediction, not user profitability.",
            "Rows represent pre-close market snapshots for resolved Polymarket condition sides.",
            "Observation rows are generated at fixed hours-before-close horizons.",
            "Only trades at or before the observation cutoff are included.",
            "No orderbook features are included because resolved-market orderbook history is sparse in the current dataset.",
            "Outcome labels are normalized using the same conservative resolver as whale scoring.",
            "Whale participation features are computed from cumulative trade and resolved-market history available on or before each observation cutoff.",
            "Historical current exposure is approximated from open shares valued at average buy price because full point-in-time position marks are not available for every user.",
        ],
    }
    return dataset_rows, metadata


def export_market_snapshot_dataset(
    session: Session,
    *,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    horizon_hours: tuple[int, ...] | list[int] | None = None,
) -> dict[str, Any]:
    """Build and write the market-level ML dataset plus metadata."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    metadata_path = metadata_path or DEFAULT_METADATA_PATH
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    rows, metadata = build_market_snapshot_dataset(session, horizon_hours=horizon_hours)
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
