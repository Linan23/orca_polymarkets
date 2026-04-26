"""Preliminary whale scoring built from normalized transactions and positions."""

from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from typing import Any

from sqlalchemy import desc, select, text
from sqlalchemy.orm import Session

from data_platform.ingest.store import UNKNOWN_USER_EXTERNAL_REF
from data_platform.ingest.lifecycle import mirror_whale_score_snapshot_part
from data_platform.models import WhaleScoreSnapshot


SCORING_VERSION = "week6_v3"
WHALE_MIN_TRADES = 10
WHALE_MIN_ACTIVE_DAYS = 3
WHALE_MIN_NOTIONAL = 5_000.0
TRUSTED_MIN_TRADES = 15
TRUSTED_MIN_ACTIVE_DAYS = 5
TRUSTED_MIN_RESOLVED_MARKETS = 2
TRUSTED_MIN_WIN_RATE = 0.60
RESOLUTION_PRICE_HIGH = 0.98
RESOLUTION_PRICE_LOW = 0.02
INSIDER_PENALTY = 0.25
WHALE_TOP_FRACTION = 0.30
TRUSTED_TOP_FRACTION = 0.05


@dataclass(frozen=True)
class WhaleMetricInput:
    """Normalized feature inputs for one canonical user."""

    user_id: int
    platform_id: int
    platform_name: str
    external_user_ref: str
    is_likely_insider: bool
    sample_trade_count: int
    distinct_markets: int
    active_trade_days: int
    total_notional: float
    current_exposure: float


@dataclass(frozen=True)
class ResolvedUserPerformance:
    """Resolved-market performance derived conservatively from captured trades."""

    user_id: int
    resolved_market_count: int
    winning_market_count: int
    realized_pnl: float
    realized_roi: float
    excluded_market_count: int


@dataclass(frozen=True)
class ResolvedMarketOutcome:
    """Resolved outcome plus source metadata for one Polymarket condition."""

    condition_ref: str
    winning_outcome_label: str
    resolution_source: str
    resolution_confidence: float
    resolution_time: datetime | None


@dataclass(frozen=True)
class WhaleScoreResult:
    """Computed preliminary whale score for one user."""

    metric: WhaleMetricInput
    resolved_performance: ResolvedUserPerformance
    raw_volume_score: float
    consistency_score: float
    profitability_score: float
    trust_score: float
    insider_penalty: float
    is_whale: bool
    is_trusted_whale: bool


TRANSACTION_METRICS_SQL = text(
    """
    SELECT
      ua.user_id,
      ua.platform_id,
      p.platform_name,
      ua.external_user_ref,
      ua.is_likely_insider,
      COUNT(tf.transaction_id) AS sample_trade_count,
      COUNT(DISTINCT tf.market_contract_id) AS distinct_markets,
      COUNT(DISTINCT DATE(tf.transaction_time AT TIME ZONE 'UTC')) AS active_trade_days,
      COALESCE(SUM(tf.notional_value), 0) AS total_notional
    FROM analytics.user_account ua
    JOIN analytics.platform p
      ON p.platform_id = ua.platform_id
    JOIN analytics.transaction_fact tf
      ON tf.user_id = ua.user_id
    WHERE ua.external_user_ref <> :unknown_user_ref
    GROUP BY
      ua.user_id,
      ua.platform_id,
      p.platform_name,
      ua.external_user_ref,
      ua.is_likely_insider
    """
)


POSITION_EXPOSURE_SQL = text(
    """
    WITH latest_positions AS (
      SELECT
        ps.user_id,
        ps.market_contract_id,
        COALESCE(
          ps.market_value,
          ABS(ps.position_size * COALESCE(ps.current_mark_price, ps.avg_entry_price)),
          ABS(ps.position_size)
        ) AS exposure_value,
        ROW_NUMBER() OVER (
          PARTITION BY ps.user_id, ps.market_contract_id
          ORDER BY ps.snapshot_time DESC, ps.position_snapshot_id DESC
        ) AS rn
      FROM analytics.position_snapshot ps
    )
    SELECT
      user_id,
      COALESCE(SUM(exposure_value), 0) AS current_exposure
    FROM latest_positions
    WHERE rn = 1
    GROUP BY user_id
    """
)


RESOLVED_MARKET_SQL = text(
    """
    SELECT
      mc.condition_ref,
      mc.outcome_a_label,
      mc.outcome_b_label,
      mc.last_trade_price,
      COALESCE(mc.end_time, me.end_time, me.closed_time) AS resolution_time,
      rp.payload AS raw_payload
    FROM analytics.market_contract mc
    JOIN analytics.market_event me
      ON me.event_id = mc.event_id
    JOIN analytics.platform p
      ON p.platform_id = mc.platform_id
    LEFT JOIN raw.api_payload rp
      ON rp.payload_id = mc.raw_payload_id
    WHERE p.platform_name = 'polymarket'
      AND mc.is_closed = TRUE
      AND mc.condition_ref IS NOT NULL
      AND mc.outcome_a_label IS NOT NULL
      AND mc.outcome_b_label IS NOT NULL
      AND (
        mc.last_trade_price IS NOT NULL
        OR rp.payload IS NOT NULL
      )
    """
)

RESOLVED_TRADE_SIGNAL_SQL = text(
    """
    SELECT
      mc.condition_ref,
      tf.outcome_label,
      MAX(tf.price) AS max_trade_price,
      MIN(tf.price) AS min_trade_price
    FROM analytics.transaction_fact tf
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = 'polymarket'
      AND mc.condition_ref IS NOT NULL
      AND tf.outcome_label IS NOT NULL
      AND tf.price IS NOT NULL
    GROUP BY
      mc.condition_ref,
      tf.outcome_label
    """
)


RESOLVED_TRANSACTION_SQL = text(
    """
    SELECT
      tf.user_id,
      mc.condition_ref,
      tf.outcome_label,
      tf.side,
      tf.transaction_time,
      COALESCE(tf.shares, 0) AS shares,
      COALESCE(tf.notional_value, 0) AS notional_value
    FROM analytics.transaction_fact tf
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = 'polymarket'
      AND mc.condition_ref IS NOT NULL
      AND tf.outcome_label IS NOT NULL
      AND tf.side IN ('buy', 'sell')
    """
)


def _percentile_rank(value: float, values: list[float]) -> float:
    """Return a simple inclusive percentile rank between 0 and 1."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return round(bisect_right(ordered, value) / len(ordered), 6)


def _percentile_rank_sorted(value: float, ordered_values: list[float]) -> float:
    """Return a percentile rank when the input values are already sorted."""
    if not ordered_values:
        return 0.0
    return round(bisect_right(ordered_values, value) / len(ordered_values), 6)


def _top_count(size: int, fraction: float) -> int:
    """Return the number of rows to keep for a top-fraction selection."""
    if size <= 0:
        return 0
    return max(1, ceil(size * fraction))


def _normalized_label(value: str | None) -> str | None:
    """Normalize outcome labels for matching across sources."""
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _parse_json_list(raw_value: Any) -> list[Any]:
    """Return a list from raw JSON/list values used by Gamma payload fields."""
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return raw_value
    try:
        decoded = json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


def _market_payload_for_condition(raw_payload: Any, condition_ref: str) -> dict[str, Any] | None:
    """Return the nested Gamma market payload matching a condition id."""
    if not isinstance(raw_payload, dict):
        return None
    if str(raw_payload.get("conditionId") or "") == condition_ref:
        return raw_payload
    markets = raw_payload.get("markets")
    if not isinstance(markets, list):
        return None
    for market_payload in markets:
        if isinstance(market_payload, dict) and str(market_payload.get("conditionId") or "") == condition_ref:
            return market_payload
    return None


def _official_outcome_from_payload(
    *,
    raw_payload: Any,
    condition_ref: str,
    outcome_a: str,
    outcome_b: str,
) -> str | None:
    """Return a resolved label from Gamma outcomePrices when the payload is decisive."""
    market_payload = _market_payload_for_condition(raw_payload, condition_ref)
    if not market_payload:
        return None
    if market_payload.get("closed") is not True:
        return None
    raw_outcomes = _parse_json_list(market_payload.get("outcomes"))
    raw_prices = _parse_json_list(market_payload.get("outcomePrices"))
    if len(raw_outcomes) < 2 or len(raw_prices) < 2:
        return None

    parsed: list[tuple[str, float]] = []
    for raw_label, raw_price in zip(raw_outcomes, raw_prices, strict=False):
        label = _normalized_label(str(raw_label))
        if label not in {outcome_a, outcome_b}:
            continue
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            continue
        parsed.append((label, price))
    if len(parsed) < 2:
        return None

    parsed.sort(key=lambda item: item[1], reverse=True)
    winning_label, winning_price = parsed[0]
    losing_price = parsed[1][1]
    if winning_price >= RESOLUTION_PRICE_HIGH and losing_price <= RESOLUTION_PRICE_LOW:
        return winning_label
    return None


def _load_current_exposure_by_user(session: Session) -> dict[int, float]:
    """Return latest known position exposure per user."""
    rows = session.execute(POSITION_EXPOSURE_SQL).mappings().all()
    return {int(row["user_id"]): float(row["current_exposure"] or 0) for row in rows}


def load_resolved_market_outcome_details(session: Session) -> dict[str, ResolvedMarketOutcome]:
    """Return confident resolved Polymarket outcomes with source metadata."""
    rows = session.execute(RESOLVED_MARKET_SQL).mappings().all()
    resolved: dict[str, ResolvedMarketOutcome] = {}
    condition_labels: dict[str, tuple[str, str]] = {}
    condition_resolution_time: dict[str, datetime | None] = {}
    for row in rows:
        condition_ref = str(row["condition_ref"])
        outcome_a = _normalized_label(str(row["outcome_a_label"]))
        outcome_b = _normalized_label(str(row["outcome_b_label"]))
        if not outcome_a or not outcome_b or outcome_a == outcome_b:
            continue
        condition_labels.setdefault(condition_ref, (outcome_a, outcome_b))
        condition_resolution_time.setdefault(condition_ref, row["resolution_time"])

        official_winner = _official_outcome_from_payload(
            raw_payload=row["raw_payload"],
            condition_ref=condition_ref,
            outcome_a=outcome_a,
            outcome_b=outcome_b,
        )
        if official_winner:
            resolved[condition_ref] = ResolvedMarketOutcome(
                condition_ref=condition_ref,
                winning_outcome_label=official_winner,
                resolution_source="gamma_outcome_prices",
                resolution_confidence=1.0,
                resolution_time=row["resolution_time"],
            )
            continue

        if row["last_trade_price"] is None or condition_ref in resolved:
            continue
        last_trade_price = float(row["last_trade_price"])
        if last_trade_price >= RESOLUTION_PRICE_HIGH:
            winner = outcome_a
        elif last_trade_price <= RESOLUTION_PRICE_LOW:
            winner = outcome_b
        else:
            winner = None
        if winner:
            resolved[condition_ref] = ResolvedMarketOutcome(
                condition_ref=condition_ref,
                winning_outcome_label=winner,
                resolution_source="last_trade_price_threshold",
                resolution_confidence=0.75,
                resolution_time=row["resolution_time"],
            )

    trade_rows = session.execute(RESOLVED_TRADE_SIGNAL_SQL).mappings().all()
    trade_stats_by_condition: dict[str, dict[str, dict[str, float]]] = {}
    for row in trade_rows:
        condition_ref = str(row["condition_ref"])
        labels = condition_labels.get(condition_ref)
        if labels is None:
            continue
        outcome_label = _normalized_label(str(row["outcome_label"]))
        if outcome_label not in labels:
            continue
        trade_stats_by_condition.setdefault(condition_ref, {})[outcome_label] = {
            "max_trade_price": float(row["max_trade_price"] or 0.0),
            "min_trade_price": float(row["min_trade_price"] or 0.0),
        }

    for condition_ref, labels in condition_labels.items():
        if condition_ref in resolved:
            continue
        trade_stats = trade_stats_by_condition.get(condition_ref, {})
        if len(trade_stats) < 2:
            continue
        outcome_a, outcome_b = labels
        outcome_a_stats = trade_stats.get(outcome_a)
        outcome_b_stats = trade_stats.get(outcome_b)
        if outcome_a_stats and outcome_b_stats:
            if (
                outcome_a_stats["max_trade_price"] >= RESOLUTION_PRICE_HIGH
                and outcome_b_stats["min_trade_price"] <= RESOLUTION_PRICE_LOW
            ):
                resolved[condition_ref] = ResolvedMarketOutcome(
                    condition_ref=condition_ref,
                    winning_outcome_label=outcome_a,
                    resolution_source="trade_price_extreme_fallback",
                    resolution_confidence=0.60,
                    resolution_time=condition_resolution_time.get(condition_ref),
                )
                continue
            if (
                outcome_b_stats["max_trade_price"] >= RESOLUTION_PRICE_HIGH
                and outcome_a_stats["min_trade_price"] <= RESOLUTION_PRICE_LOW
            ):
                resolved[condition_ref] = ResolvedMarketOutcome(
                    condition_ref=condition_ref,
                    winning_outcome_label=outcome_b,
                    resolution_source="trade_price_extreme_fallback",
                    resolution_confidence=0.60,
                    resolution_time=condition_resolution_time.get(condition_ref),
                )
    return resolved


def load_resolved_market_outcomes(session: Session) -> dict[str, str]:
    """Return confident resolved Polymarket outcome labels keyed by condition id."""
    return {
        condition_ref: detail.winning_outcome_label
        for condition_ref, detail in load_resolved_market_outcome_details(session).items()
    }


def load_resolved_user_performance(
    session: Session,
    *,
    resolved_outcomes: dict[str, str] | None = None,
    start_time: datetime | None = None,
) -> tuple[dict[int, ResolvedUserPerformance], dict[str, int]]:
    """Return conservative resolved-market performance metrics by user."""
    resolved_outcomes = resolved_outcomes or load_resolved_market_outcomes(session)
    if not resolved_outcomes:
        return {}, {"resolved_markets_available": 0, "resolved_markets_observed": 0, "profitability_users": 0}

    rows = session.execute(RESOLVED_TRANSACTION_SQL).mappings().all()
    per_user_condition: dict[int, dict[str, dict[str, dict[str, float]]]] = {}
    observed_conditions: set[str] = set()
    for row in rows:
        condition_ref = str(row["condition_ref"])
        if condition_ref not in resolved_outcomes:
            continue
        transaction_time = row["transaction_time"]
        if start_time is not None and transaction_time is not None and transaction_time < start_time:
            continue
        outcome_label = _normalized_label(str(row["outcome_label"]))
        if not outcome_label:
            continue
        observed_conditions.add(condition_ref)
        user_bucket = per_user_condition.setdefault(int(row["user_id"]), {})
        condition_bucket = user_bucket.setdefault(condition_ref, {})
        outcome_bucket = condition_bucket.setdefault(
            outcome_label,
            {"bought_shares": 0.0, "sold_shares": 0.0, "buy_notional": 0.0, "sell_notional": 0.0},
        )
        shares = float(row["shares"] or 0)
        notional_value = float(row["notional_value"] or 0)
        if row["side"] == "buy":
            outcome_bucket["bought_shares"] += shares
            outcome_bucket["buy_notional"] += notional_value
        else:
            outcome_bucket["sold_shares"] += shares
            outcome_bucket["sell_notional"] += notional_value

    performance_by_user: dict[int, ResolvedUserPerformance] = {}
    for user_id, condition_map in per_user_condition.items():
        resolved_market_count = 0
        winning_market_count = 0
        excluded_market_count = 0
        realized_pnl_total = 0.0
        invested_total = 0.0
        for condition_ref, outcome_map in condition_map.items():
            winning_outcome = resolved_outcomes.get(condition_ref)
            if not winning_outcome:
                continue
            if any(values["sold_shares"] > values["bought_shares"] + 1e-9 for values in outcome_map.values()):
                excluded_market_count += 1
                continue

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
            resolved_market_count += 1
            invested_total += condition_invested
            realized_pnl_total += condition_realized_pnl
            if condition_realized_pnl > 0:
                winning_market_count += 1

        if resolved_market_count == 0 and excluded_market_count == 0:
            continue
        realized_roi = (realized_pnl_total / invested_total) if invested_total > 0 else 0.0
        performance_by_user[user_id] = ResolvedUserPerformance(
            user_id=user_id,
            resolved_market_count=resolved_market_count,
            winning_market_count=winning_market_count,
            realized_pnl=round(realized_pnl_total, 8),
            realized_roi=round(realized_roi, 8),
            excluded_market_count=excluded_market_count,
        )

    return performance_by_user, {
        "resolved_markets_available": len(resolved_outcomes),
        "resolved_markets_observed": len(observed_conditions),
        "profitability_users": len(
            [item for item in performance_by_user.values() if item.resolved_market_count > 0]
        ),
    }


def load_whale_metric_inputs(session: Session) -> list[WhaleMetricInput]:
    """Load feature inputs for whale scoring from the normalized source layer."""
    exposure_by_user = _load_current_exposure_by_user(session)
    rows = session.execute(
        TRANSACTION_METRICS_SQL,
        {"unknown_user_ref": UNKNOWN_USER_EXTERNAL_REF},
    ).mappings()
    metrics: list[WhaleMetricInput] = []
    for row in rows:
        metrics.append(
            WhaleMetricInput(
                user_id=int(row["user_id"]),
                platform_id=int(row["platform_id"]),
                platform_name=str(row["platform_name"]),
                external_user_ref=str(row["external_user_ref"]),
                is_likely_insider=bool(row["is_likely_insider"]),
                sample_trade_count=int(row["sample_trade_count"] or 0),
                distinct_markets=int(row["distinct_markets"] or 0),
                active_trade_days=int(row["active_trade_days"] or 0),
                total_notional=float(row["total_notional"] or 0),
                current_exposure=float(exposure_by_user.get(int(row["user_id"]), 0.0)),
            )
        )
    return metrics


def _compute_platform_scores(
    metrics: list[WhaleMetricInput],
    *,
    resolved_performance_by_user: dict[int, ResolvedUserPerformance],
) -> list[WhaleScoreResult]:
    """Compute per-platform whale scores from normalized metrics."""
    if not metrics:
        return []

    total_notionals = [item.total_notional for item in metrics]
    market_counts = [float(item.distinct_markets) for item in metrics]
    active_trade_days = [float(item.active_trade_days) for item in metrics]
    exposures = [item.current_exposure for item in metrics]
    ordered_total_notionals = sorted(total_notionals)
    ordered_market_counts = sorted(market_counts)
    ordered_active_trade_days = sorted(active_trade_days)
    ordered_exposures = sorted(exposures)
    metric_user_ids = {item.user_id for item in metrics}
    pnl_values = [
        performance.realized_pnl
        for performance in resolved_performance_by_user.values()
        if performance.user_id in metric_user_ids and performance.resolved_market_count > 0
    ]
    roi_values = [
        performance.realized_roi
        for performance in resolved_performance_by_user.values()
        if performance.user_id in metric_user_ids and performance.resolved_market_count > 0
    ]
    win_rate_values = [
        (performance.winning_market_count / performance.resolved_market_count)
        for performance in resolved_performance_by_user.values()
        if performance.user_id in metric_user_ids and performance.resolved_market_count > 0
    ]
    ordered_pnl_values = sorted(pnl_values)
    ordered_roi_values = sorted(roi_values)
    ordered_win_rate_values = sorted(win_rate_values)

    scored: list[WhaleScoreResult] = []
    for item in metrics:
        resolved_performance = resolved_performance_by_user.get(
            item.user_id,
            ResolvedUserPerformance(
                user_id=item.user_id,
                resolved_market_count=0,
                winning_market_count=0,
                realized_pnl=0.0,
                realized_roi=0.0,
                excluded_market_count=0,
            ),
        )
        raw_volume_score = _percentile_rank_sorted(item.total_notional, ordered_total_notionals)
        market_breadth_score = _percentile_rank_sorted(float(item.distinct_markets), ordered_market_counts)
        consistency_score = _percentile_rank_sorted(float(item.active_trade_days), ordered_active_trade_days)
        current_exposure_score = _percentile_rank_sorted(item.current_exposure, ordered_exposures)
        insider_penalty = INSIDER_PENALTY if item.is_likely_insider else 0.0
        if resolved_performance.resolved_market_count > 0:
            realized_pnl_score = _percentile_rank_sorted(resolved_performance.realized_pnl, ordered_pnl_values)
            realized_roi_score = _percentile_rank_sorted(resolved_performance.realized_roi, ordered_roi_values)
            win_rate = resolved_performance.winning_market_count / resolved_performance.resolved_market_count
            win_rate_score = _percentile_rank_sorted(win_rate, ordered_win_rate_values)
            profitability_score = round(
                (0.40 * realized_pnl_score) + (0.30 * realized_roi_score) + (0.30 * win_rate_score),
                6,
            )
        else:
            profitability_score = 0.0
        trust_score = max(
            0.0,
            round(
                (0.50 * raw_volume_score)
                + (0.20 * market_breadth_score)
                + (0.20 * consistency_score)
                + (0.10 * current_exposure_score)
                + (0.15 * profitability_score)
                - insider_penalty,
                6,
            ),
        )
        scored.append(
            WhaleScoreResult(
                metric=item,
                resolved_performance=resolved_performance,
                raw_volume_score=raw_volume_score,
                consistency_score=consistency_score,
                profitability_score=profitability_score,
                trust_score=trust_score,
                insider_penalty=insider_penalty,
                is_whale=False,
                is_trusted_whale=False,
            )
        )

    eligible_whales = [
        item
        for item in scored
        if item.metric.sample_trade_count >= WHALE_MIN_TRADES
        and item.metric.active_trade_days >= WHALE_MIN_ACTIVE_DAYS
        and item.metric.total_notional >= WHALE_MIN_NOTIONAL
        and not item.metric.is_likely_insider
    ]
    eligible_whales.sort(
        key=lambda item: (
            item.trust_score,
            item.metric.total_notional,
            item.metric.sample_trade_count,
        ),
        reverse=True,
    )
    whale_ids = {item.metric.user_id for item in eligible_whales[: _top_count(len(eligible_whales), WHALE_TOP_FRACTION)]}

    eligible_trusted = [
        item
        for item in eligible_whales
        if item.metric.sample_trade_count >= TRUSTED_MIN_TRADES
        and item.metric.active_trade_days >= TRUSTED_MIN_ACTIVE_DAYS
        and item.resolved_performance.resolved_market_count >= TRUSTED_MIN_RESOLVED_MARKETS
        and item.profitability_score > 0
        and (
            item.resolved_performance.winning_market_count / item.resolved_performance.resolved_market_count
        )
        >= TRUSTED_MIN_WIN_RATE
    ]
    eligible_trusted.sort(
        key=lambda item: (
            item.trust_score,
            item.metric.total_notional,
            item.metric.sample_trade_count,
        ),
        reverse=True,
    )
    trusted_ids = {
        item.metric.user_id for item in eligible_trusted[: _top_count(len(eligible_trusted), TRUSTED_TOP_FRACTION)]
    }

    final_results: list[WhaleScoreResult] = []
    for item in scored:
        final_results.append(
            WhaleScoreResult(
                metric=item.metric,
                resolved_performance=item.resolved_performance,
                raw_volume_score=item.raw_volume_score,
                consistency_score=item.consistency_score,
                profitability_score=item.profitability_score,
                trust_score=item.trust_score,
                insider_penalty=item.insider_penalty,
                is_whale=item.metric.user_id in whale_ids,
                is_trusted_whale=item.metric.user_id in trusted_ids,
            )
        )
    return final_results


def compute_whale_scores(
    metrics: list[WhaleMetricInput],
    *,
    resolved_performance_by_user: dict[int, ResolvedUserPerformance],
) -> list[WhaleScoreResult]:
    """Return whale scores for one platform from supplied point-in-time metrics."""
    return _compute_platform_scores(
        metrics,
        resolved_performance_by_user=resolved_performance_by_user,
    )


def build_whale_score_snapshot(session: Session, *, scoring_version: str = SCORING_VERSION) -> dict[str, Any]:
    """Compute and persist one whale score snapshot from normalized source data."""
    snapshot_time = datetime.now(timezone.utc)
    metrics = load_whale_metric_inputs(session)
    resolved_outcomes = load_resolved_market_outcomes(session)
    resolved_performance_by_user, profitability_summary = load_resolved_user_performance(
        session,
        resolved_outcomes=resolved_outcomes,
    )
    platform_groups: dict[int, list[WhaleMetricInput]] = {}
    for item in metrics:
        platform_groups.setdefault(item.platform_id, []).append(item)

    platform_summaries: dict[str, dict[str, int]] = {}
    rows_written = 0
    for platform_metrics in platform_groups.values():
        scored_results = _compute_platform_scores(
            platform_metrics,
            resolved_performance_by_user=resolved_performance_by_user,
        )
        if not scored_results:
            continue
        platform_name = scored_results[0].metric.platform_name
        platform_summaries[platform_name] = {
            "scored_users": len(scored_results),
            "whales": sum(1 for item in scored_results if item.is_whale),
            "trusted_whales": sum(1 for item in scored_results if item.is_trusted_whale),
            "profitability_users": sum(
                1 for item in scored_results if item.resolved_performance.resolved_market_count > 0
            ),
        }
        for item in scored_results:
            row = WhaleScoreSnapshot(
                user_id=item.metric.user_id,
                platform_id=item.metric.platform_id,
                snapshot_time=snapshot_time,
                raw_volume_score=item.raw_volume_score,
                consistency_score=item.consistency_score,
                profitability_score=item.profitability_score,
                trust_score=item.trust_score,
                insider_penalty=item.insider_penalty,
                is_whale=item.is_whale,
                is_trusted_whale=item.is_trusted_whale,
                sample_trade_count=item.metric.sample_trade_count,
                scoring_version=scoring_version,
            )
            session.add(row)
            session.flush()
            mirror_whale_score_snapshot_part(session, row)
            rows_written += 1

    return {
        "snapshot_time": snapshot_time.isoformat(),
        "scoring_version": scoring_version,
        "rows_written": rows_written,
        "platforms": platform_summaries,
        "resolved_markets_available": len(resolved_outcomes),
        "profitability_summary": profitability_summary,
        "trusted_whale_logic": "enabled_only_for_users_with_resolved_market_history",
    }


def latest_whale_scores_by_user(session: Session) -> dict[int, WhaleScoreSnapshot]:
    """Return the latest coherent whale scoring batch keyed by user."""
    latest_batch = session.execute(
        select(
            WhaleScoreSnapshot.snapshot_time,
            WhaleScoreSnapshot.scoring_version,
        )
        .order_by(
            desc(WhaleScoreSnapshot.snapshot_time),
            desc(WhaleScoreSnapshot.whale_score_snapshot_id),
        )
        .limit(1)
    ).first()
    if latest_batch is None:
        return {}
    rows = session.scalars(
        select(WhaleScoreSnapshot).where(
            WhaleScoreSnapshot.snapshot_time == latest_batch.snapshot_time,
            WhaleScoreSnapshot.scoring_version == latest_batch.scoring_version,
        )
    ).all()
    return {row.user_id: row for row in rows}
