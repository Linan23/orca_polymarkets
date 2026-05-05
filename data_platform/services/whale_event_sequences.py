"""Semi-live whale event sequence features for trend research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


DEFAULT_PLATFORM = "polymarket"
DEFAULT_LOOKBACK_HOURS = 24
MAX_LOOKBACK_HOURS = 72
DEFAULT_BUCKET_HOURS = 1
MAX_QUERY_ROWS = 50_000
MAX_EVENTS_PER_SIDE = 18

PHYSICAL_SPORTS_TERMS = (
    "nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "tennis", "golf",
    "cricket", "rugby", "baseball", "basketball", "hockey", "formula 1", "f1",
)
ESPORTS_TERMS = ("esports", "e-sports", "video game", "video-games", "gaming")


def _looks_like_physical_sports_market(texts: list[Any], *, category: Any = None) -> bool:
    """Return true for physical sports while keeping esports/video-games in scope."""
    joined = " ".join(str(value or "") for value in [category, *texts]).casefold()
    if any(term in joined for term in ESPORTS_TERMS):
        return False
    return any(term in joined for term in PHYSICAL_SPORTS_TERMS)


AS_OF_SQL = text(
    """
    SELECT MAX(tf.transaction_time) AS as_of
    FROM analytics.transaction_fact tf
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = :platform_name
    """
)

MARKET_AS_OF_SQL = text(
    """
    SELECT MAX(tf.transaction_time) AS as_of
    FROM analytics.transaction_fact tf
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = :platform_name
      AND LOWER(COALESCE(mc.market_slug, '')) = :market_slug
    """
)

LATEST_WHALE_BATCH_SQL = text(
    """
    SELECT
      w.snapshot_time,
      w.scoring_version
    FROM analytics.whale_score_snapshot w
    JOIN analytics.platform p
      ON p.platform_id = w.platform_id
    WHERE p.platform_name = :platform_name
    ORDER BY w.snapshot_time DESC, w.created_at DESC, w.whale_score_snapshot_id DESC
    LIMIT 1
    """
)

WHALE_EVENTS_SQL = text(
    """
    WITH latest_scores AS (
      SELECT
        w.user_id,
        w.platform_id,
        w.trust_score,
        w.profitability_score,
        w.sample_trade_count,
        w.is_whale,
        w.is_trusted_whale
      FROM analytics.whale_score_snapshot w
      JOIN analytics.platform p
        ON p.platform_id = w.platform_id
      WHERE p.platform_name = :platform_name
        AND w.snapshot_time = CAST(:score_snapshot_time AS TIMESTAMPTZ)
        AND w.scoring_version = :scoring_version
        AND (w.is_whale = TRUE OR w.is_trusted_whale = TRUE)
        AND (:trusted_only = FALSE OR w.is_trusted_whale = TRUE)
    )
    SELECT
      tf.transaction_id,
      tf.user_id,
      tf.market_contract_id,
      tf.event_id,
      tf.transaction_time,
      tf.side,
      tf.outcome_label,
      tf.price,
      tf.shares,
      tf.notional_value,
      tf.sequence_ts,
      ls.trust_score,
      ls.profitability_score,
      ls.sample_trade_count,
      ls.is_trusted_whale,
      mc.market_slug,
      mc.question,
      mc.last_trade_price,
      mc.is_closed,
      COALESCE(mc.end_time, me.end_time, me.closed_time) AS market_end_time,
      me.title AS event_title,
      me.slug AS event_slug,
      me.category AS event_category
    FROM analytics.transaction_fact tf
    JOIN latest_scores ls
      ON ls.user_id = tf.user_id
     AND ls.platform_id = tf.platform_id
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.market_event me
      ON me.event_id = tf.event_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = :platform_name
      AND tf.transaction_time >= CAST(:cutoff_time AS TIMESTAMPTZ)
      AND tf.transaction_time <= CAST(:as_of AS TIMESTAMPTZ)
      AND LOWER(COALESCE(tf.side, '')) IN ('buy', 'sell')
      AND tf.outcome_label IS NOT NULL
      AND COALESCE(tf.notional_value, 0) > 0
    ORDER BY tf.transaction_time DESC, tf.transaction_id DESC
    LIMIT :max_rows
    """
)

WHALE_MARKET_EVENTS_SQL = text(
    """
    WITH latest_scores AS (
      SELECT
        w.user_id,
        w.platform_id,
        w.trust_score,
        w.profitability_score,
        w.sample_trade_count,
        w.is_whale,
        w.is_trusted_whale
      FROM analytics.whale_score_snapshot w
      JOIN analytics.platform p
        ON p.platform_id = w.platform_id
      WHERE p.platform_name = :platform_name
        AND w.snapshot_time = CAST(:score_snapshot_time AS TIMESTAMPTZ)
        AND w.scoring_version = :scoring_version
        AND (w.is_whale = TRUE OR w.is_trusted_whale = TRUE)
        AND (:trusted_only = FALSE OR w.is_trusted_whale = TRUE)
    )
    SELECT
      tf.transaction_id,
      tf.user_id,
      tf.market_contract_id,
      tf.event_id,
      tf.transaction_time,
      tf.side,
      tf.outcome_label,
      tf.price,
      tf.shares,
      tf.notional_value,
      tf.sequence_ts,
      ls.trust_score,
      ls.profitability_score,
      ls.sample_trade_count,
      ls.is_trusted_whale,
      mc.market_slug,
      mc.question,
      mc.last_trade_price,
      mc.is_closed,
      COALESCE(mc.end_time, me.end_time, me.closed_time) AS market_end_time,
      me.title AS event_title,
      me.slug AS event_slug,
      me.category AS event_category
    FROM analytics.transaction_fact tf
    JOIN latest_scores ls
      ON ls.user_id = tf.user_id
     AND ls.platform_id = tf.platform_id
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.market_event me
      ON me.event_id = tf.event_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = :platform_name
      AND LOWER(COALESCE(mc.market_slug, '')) = :market_slug
      AND tf.transaction_time >= CAST(:cutoff_time AS TIMESTAMPTZ)
      AND tf.transaction_time <= CAST(:as_of AS TIMESTAMPTZ)
      AND LOWER(COALESCE(tf.side, '')) IN ('buy', 'sell')
      AND tf.outcome_label IS NOT NULL
      AND COALESCE(tf.notional_value, 0) > 0
    ORDER BY tf.transaction_time DESC, tf.transaction_id DESC
    LIMIT :max_rows
    """
)


@dataclass(frozen=True)
class WhaleEventRow:
    """Normalized transaction row used to build a market-side sequence."""

    transaction_id: int
    user_id: int
    market_contract_id: int
    transaction_time: datetime
    side: str
    outcome_label: str
    price: float
    shares: float
    notional_value: float
    sequence_ts: int | None
    trust_score: float
    profitability_score: float
    sample_trade_count: int
    is_trusted_whale: bool
    market_slug: str
    question: str
    last_trade_price: float | None
    is_closed: bool
    market_end_time: datetime | None
    event_title: str
    event_slug: str | None
    event_category: str | None


def _safe_float(value: Any) -> float:
    """Return a finite float or zero."""
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return numeric if math.isfinite(numeric) else 0.0


def _safe_int(value: Any) -> int:
    """Return an int or zero."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _iso(value: datetime | None) -> str | None:
    """Serialize optional datetimes."""
    return value.isoformat() if value else None


def _hours_between(later: datetime, earlier: datetime) -> float:
    """Return non-negative hours between two timestamps."""
    return max((later - earlier).total_seconds() / 3600.0, 0.0)


def _pct(value: float | None) -> float | None:
    """Convert probability values to dashboard percentage units."""
    if value is None:
        return None
    return round(float(value) * 100.0, 4)


def _normalized_label(value: str | None) -> str:
    """Normalize outcome labels for market-side grouping."""
    return str(value or "").strip().lower()


def _is_sports_market(row: WhaleEventRow) -> bool:
    """Return whether the market is an excluded physical sports market."""
    return _looks_like_physical_sports_market(
        [row.market_slug, row.question, row.event_title, row.event_slug],
        category=row.event_category,
    )


def _row_from_mapping(row: dict[str, Any]) -> WhaleEventRow:
    """Normalize a SQL mapping into a dataclass."""
    return WhaleEventRow(
        transaction_id=int(row["transaction_id"]),
        user_id=int(row["user_id"]),
        market_contract_id=int(row["market_contract_id"]),
        transaction_time=row["transaction_time"],
        side=str(row["side"] or "").strip().lower(),
        outcome_label=str(row["outcome_label"] or "").strip(),
        price=_safe_float(row["price"]),
        shares=_safe_float(row["shares"]),
        notional_value=_safe_float(row["notional_value"]),
        sequence_ts=row["sequence_ts"],
        trust_score=_safe_float(row["trust_score"]),
        profitability_score=_safe_float(row["profitability_score"]),
        sample_trade_count=_safe_int(row["sample_trade_count"]),
        is_trusted_whale=bool(row["is_trusted_whale"]),
        market_slug=str(row["market_slug"] or ""),
        question=str(row["question"] or ""),
        last_trade_price=float(row["last_trade_price"]) if row["last_trade_price"] is not None else None,
        is_closed=bool(row["is_closed"]),
        market_end_time=row["market_end_time"],
        event_title=str(row["event_title"] or ""),
        event_slug=str(row["event_slug"] or "") if row["event_slug"] is not None else None,
        event_category=str(row["event_category"] or "") if row["event_category"] is not None else None,
    )


def _event_payload(event: WhaleEventRow, *, as_of: datetime) -> dict[str, Any]:
    """Return a compact event payload for the dashboard."""
    weighted_notional = event.notional_value * max(event.trust_score, 0.0)
    return {
        "transaction_id": event.transaction_id,
        "user_id": event.user_id,
        "event_time": event.transaction_time.isoformat(),
        "age_hours": round(_hours_between(as_of, event.transaction_time), 4),
        "side": event.side,
        "event_type": "entry" if event.side == "buy" else "exit",
        "outcome_label": event.outcome_label,
        "price_pct": _pct(event.price),
        "shares": round(event.shares, 6),
        "notional_value": round(event.notional_value, 6),
        "weighted_notional": round(weighted_notional, 6),
        "trust_score": round(event.trust_score, 6),
        "profitability_score": round(event.profitability_score, 6),
        "sample_trade_count": event.sample_trade_count,
        "is_trusted_whale": event.is_trusted_whale,
    }


def _anchor_payload(event: WhaleEventRow | None, *, as_of: datetime, event_type: str) -> dict[str, Any] | None:
    """Return the latest entry/exit anchor payload for a market-side sequence."""
    if event is None:
        return None
    return {
        "event_type": event_type,
        "event_time": event.transaction_time.isoformat(),
        "age_hours": round(_hours_between(as_of, event.transaction_time), 4),
        "odds_pct": _pct(event.price),
        "notional_value": round(event.notional_value, 6),
        "weighted_notional": round(event.notional_value * max(event.trust_score, 0.0), 6),
        "trust_score": round(event.trust_score, 6),
        "is_trusted_whale": event.is_trusted_whale,
        "user_id": event.user_id,
    }


def _window_feature_rows(events: list[WhaleEventRow], *, as_of: datetime, hours: int) -> dict[str, Any]:
    """Return entry/exit sequence features for one rolling window."""
    cutoff = as_of - timedelta(hours=hours)
    selected = [event for event in events if event.transaction_time >= cutoff]
    entry_events = [event for event in selected if event.side == "buy"]
    exit_events = [event for event in selected if event.side == "sell"]
    trusted_events = [event for event in selected if event.is_trusted_whale]
    weighted_buy_pressure = sum(event.notional_value * max(event.trust_score, 0.0) for event in entry_events)
    weighted_sell_pressure = sum(event.notional_value * max(event.trust_score, 0.0) for event in exit_events)
    entry_notional = sum(event.notional_value for event in entry_events)
    exit_notional = sum(event.notional_value for event in exit_events)
    distinct_users = {event.user_id for event in selected}
    trusted_users = {event.user_id for event in trusted_events}
    latest_entry_time = max((event.transaction_time for event in entry_events), default=None)
    latest_exit_time = max((event.transaction_time for event in exit_events), default=None)

    return {
        "window_hours": hours,
        "event_count": len(selected),
        "entry_count": len(entry_events),
        "exit_count": len(exit_events),
        "net_entry_count": len(entry_events) - len(exit_events),
        "distinct_whales": len(distinct_users),
        "trusted_event_count": len(trusted_events),
        "trusted_distinct_whales": len(trusted_users),
        "entry_notional": round(entry_notional, 6),
        "exit_notional": round(exit_notional, 6),
        "net_notional": round(entry_notional - exit_notional, 6),
        "weighted_buy_pressure": round(weighted_buy_pressure, 6),
        "weighted_sell_pressure": round(weighted_sell_pressure, 6),
        "weighted_net_pressure": round(weighted_buy_pressure - weighted_sell_pressure, 6),
        "entry_exit_ratio": round(len(entry_events) / max(len(exit_events), 1), 6),
        "trusted_event_share_pct": round((len(trusted_events) / len(selected)) * 100.0, 4) if selected else 0.0,
        "latest_entry_time": _iso(latest_entry_time),
        "latest_exit_time": _iso(latest_exit_time),
        "latest_entry_age_hours": round(_hours_between(as_of, latest_entry_time), 4) if latest_entry_time else None,
        "latest_exit_age_hours": round(_hours_between(as_of, latest_exit_time), 4) if latest_exit_time else None,
    }


def _bucket_payload(events: list[WhaleEventRow], *, as_of: datetime, lookback_hours: int, bucket_hours: int) -> list[dict[str, Any]]:
    """Return fixed-width chronological sequence buckets."""
    bucket_count = max(1, math.ceil(lookback_hours / bucket_hours))
    buckets: list[dict[str, Any]] = []
    for bucket_index in range(bucket_count):
        bucket_end = as_of - timedelta(hours=bucket_index * bucket_hours)
        bucket_start = bucket_end - timedelta(hours=bucket_hours)
        selected = [
            event
            for event in events
            if bucket_start <= event.transaction_time < bucket_end
            or (bucket_index == 0 and event.transaction_time == as_of)
        ]
        entry_events = [event for event in selected if event.side == "buy"]
        exit_events = [event for event in selected if event.side == "sell"]
        weighted_buy_pressure = sum(event.notional_value * max(event.trust_score, 0.0) for event in entry_events)
        weighted_sell_pressure = sum(event.notional_value * max(event.trust_score, 0.0) for event in exit_events)
        buckets.append(
            {
                "bucket_start": bucket_start.isoformat(),
                "bucket_end": bucket_end.isoformat(),
                "relative_start_hours": round(-1.0 * (bucket_index + 1) * bucket_hours, 4),
                "relative_end_hours": round(-1.0 * bucket_index * bucket_hours, 4),
                "event_count": len(selected),
                "entry_count": len(entry_events),
                "exit_count": len(exit_events),
                "entry_notional": round(sum(event.notional_value for event in entry_events), 6),
                "exit_notional": round(sum(event.notional_value for event in exit_events), 6),
                "weighted_net_pressure": round(weighted_buy_pressure - weighted_sell_pressure, 6),
                "trusted_event_count": sum(1 for event in selected if event.is_trusted_whale),
            }
        )
    return list(reversed(buckets))


def _flatten_feature_vector(window_features: dict[int, dict[str, Any]]) -> dict[str, float | int | None]:
    """Return ML-ready flat sequence features for future model experiments."""
    feature_vector: dict[str, float | int | None] = {}
    for hours, features in window_features.items():
        prefix = f"whale_sequence_{hours}h"
        for key in (
            "event_count",
            "entry_count",
            "exit_count",
            "net_entry_count",
            "distinct_whales",
            "trusted_event_count",
            "trusted_distinct_whales",
            "entry_notional",
            "exit_notional",
            "net_notional",
            "weighted_buy_pressure",
            "weighted_sell_pressure",
            "weighted_net_pressure",
            "entry_exit_ratio",
            "trusted_event_share_pct",
            "latest_entry_age_hours",
            "latest_exit_age_hours",
        ):
            feature_vector[f"{prefix}_{key}"] = features.get(key)

    one_hour = window_features.get(1, {})
    twenty_four = window_features.get(24, {})
    feature_vector["whale_sequence_entry_burst_1h_vs_24h"] = round(
        float(one_hour.get("entry_count") or 0) / max(float(twenty_four.get("entry_count") or 0), 1.0),
        6,
    )
    feature_vector["whale_sequence_net_pressure_burst_1h_vs_24h"] = round(
        float(one_hour.get("weighted_net_pressure") or 0.0) / max(abs(float(twenty_four.get("weighted_net_pressure") or 0.0)), 1.0),
        6,
    )
    return feature_vector


def _sequence_payload(events: list[WhaleEventRow], *, as_of: datetime, lookback_hours: int, bucket_hours: int) -> dict[str, Any]:
    """Return one market-side sequence payload."""
    ordered = sorted(events, key=lambda event: (event.transaction_time, event.transaction_id))
    latest_event = ordered[-1]
    latest_entry = next((event for event in reversed(ordered) if event.side == "buy"), None)
    latest_exit = next((event for event in reversed(ordered) if event.side == "sell"), None)
    window_features = {
        hours: _window_feature_rows(ordered, as_of=as_of, hours=hours)
        for hours in (1, 6, 12, 24)
        if hours <= max(lookback_hours, 1)
    }
    if 24 not in window_features:
        window_features[24] = _window_feature_rows(ordered, as_of=as_of, hours=min(lookback_hours, 24))
    feature_vector = _flatten_feature_vector(window_features)
    signal_event = latest_entry or latest_event
    signal_type = "entry" if latest_entry and (not latest_exit or latest_entry.transaction_time >= latest_exit.transaction_time) else "exit"
    if latest_entry and latest_exit and latest_entry.transaction_time == latest_exit.transaction_time:
        signal_type = "mixed"

    score_key = (
        abs(float(feature_vector.get("whale_sequence_24h_weighted_net_pressure") or 0.0)),
        float(feature_vector.get("whale_sequence_24h_event_count") or 0.0),
        float(feature_vector.get("whale_sequence_24h_entry_notional") or 0.0),
    )

    return {
        "market_contract_id": latest_event.market_contract_id,
        "market_slug": latest_event.market_slug,
        "question": latest_event.question,
        "event_title": latest_event.event_title,
        "event_category": latest_event.event_category,
        "side_label": latest_event.outcome_label,
        "market_status": "closed" if latest_event.is_closed else "open",
        "market_end_time": _iso(latest_event.market_end_time),
        "current_market_price_pct": _pct(latest_event.last_trade_price),
        "signal": {
            "signal_type": signal_type,
            "signal_time": signal_event.transaction_time.isoformat(),
            "signal_age_hours": round(_hours_between(as_of, signal_event.transaction_time), 4),
            "signal_odds_pct": _pct(signal_event.price),
            "signal_notional": round(signal_event.notional_value, 6),
            "signal_weighted_notional": round(signal_event.notional_value * max(signal_event.trust_score, 0.0), 6),
            "signal_trust_score": round(signal_event.trust_score, 6),
            "signal_is_trusted_whale": signal_event.is_trusted_whale,
        },
        "entry_anchor": _anchor_payload(latest_entry, as_of=as_of, event_type="entry"),
        "exit_anchor": _anchor_payload(latest_exit, as_of=as_of, event_type="exit"),
        "window_features": {f"{hours}h": features for hours, features in sorted(window_features.items())},
        "feature_vector": feature_vector,
        "hourly_buckets": _bucket_payload(ordered, as_of=as_of, lookback_hours=lookback_hours, bucket_hours=bucket_hours),
        "recent_events": [
            _event_payload(event, as_of=as_of)
            for event in sorted(ordered, key=lambda event: (event.transaction_time, event.transaction_id), reverse=True)[
                :MAX_EVENTS_PER_SIDE
            ]
        ],
        "_sort_key": score_key,
    }


def _latest_whale_score_batch(session: Session, *, platform_name: str) -> dict[str, Any] | None:
    """Return the latest whale score batch for a platform."""
    return session.execute(
        LATEST_WHALE_BATCH_SQL,
        {"platform_name": platform_name},
    ).mappings().first()


def whale_event_sequence_for_market(
    session: Session,
    *,
    market_slug: str,
    lookback_hours: int = MAX_LOOKBACK_HOURS,
    bucket_hours: int = DEFAULT_BUCKET_HOURS,
    trusted_only: bool = False,
    platform_name: str = DEFAULT_PLATFORM,
) -> dict[str, Any]:
    """Return semi-live whale entry/exit sequences for one market slug."""
    normalized_market_slug = str(market_slug or "").strip().casefold()
    clean_lookback_hours = min(max(int(lookback_hours), 1), MAX_LOOKBACK_HOURS)
    clean_bucket_hours = min(max(int(bucket_hours), 1), clean_lookback_hours)
    clean_platform = (platform_name or DEFAULT_PLATFORM).strip().casefold()
    generated_at = datetime.now(timezone.utc)
    if not normalized_market_slug:
        return {
            "available": False,
            "reason": "missing_market_slug",
            "generated_at": generated_at.isoformat(),
            "as_of": None,
            "platform": clean_platform,
            "market_slug": normalized_market_slug,
            "items": [],
        }

    as_of_row = session.execute(
        MARKET_AS_OF_SQL,
        {
            "platform_name": clean_platform,
            "market_slug": normalized_market_slug,
        },
    ).mappings().first()
    as_of = as_of_row.get("as_of") if as_of_row else None
    if as_of is None:
        return {
            "available": False,
            "reason": f"No transaction data available for {clean_platform}.",
            "generated_at": generated_at.isoformat(),
            "as_of": None,
            "platform": clean_platform,
            "market_slug": normalized_market_slug,
            "items": [],
        }

    score_batch = _latest_whale_score_batch(session, platform_name=clean_platform)
    if not score_batch:
        return {
            "available": False,
            "reason": f"No whale score snapshot is available for {clean_platform}.",
            "generated_at": generated_at.isoformat(),
            "as_of": as_of.isoformat(),
            "platform": clean_platform,
            "market_slug": normalized_market_slug,
            "items": [],
        }

    cutoff_time = as_of - timedelta(hours=clean_lookback_hours)
    rows = session.execute(
        WHALE_MARKET_EVENTS_SQL,
        {
            "platform_name": clean_platform,
            "score_snapshot_time": score_batch["snapshot_time"],
            "scoring_version": score_batch["scoring_version"],
            "market_slug": normalized_market_slug,
            "cutoff_time": cutoff_time,
            "as_of": as_of,
            "trusted_only": bool(trusted_only),
            "max_rows": MAX_QUERY_ROWS,
        },
    ).mappings().all()

    grouped: dict[tuple[int, str], list[WhaleEventRow]] = {}
    excluded_sports_rows = 0
    for raw_row in rows:
        event = _row_from_mapping(raw_row)
        if _is_sports_market(event):
            excluded_sports_rows += 1
            continue
        side_label = _normalized_label(event.outcome_label)
        if not side_label:
            continue
        grouped.setdefault((event.market_contract_id, side_label), []).append(event)

    sequence_items = [
        _sequence_payload(events, as_of=as_of, lookback_hours=clean_lookback_hours, bucket_hours=clean_bucket_hours)
        for events in grouped.values()
        if events
    ]
    sequence_items.sort(key=lambda item: item.pop("_sort_key"), reverse=True)

    return {
        "available": bool(sequence_items),
        "reason": "" if sequence_items else "no_recent_whale_entry_exit_sequence_for_market",
        "generated_at": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "platform": clean_platform,
        "market_slug": normalized_market_slug,
        "score_snapshot_time": score_batch["snapshot_time"].isoformat(),
        "scoring_version": score_batch["scoring_version"],
        "lookback_hours": clean_lookback_hours,
        "bucket_hours": clean_bucket_hours,
        "trusted_only": bool(trusted_only),
        "semi_live": True,
        "cache_ttl_seconds": 30,
        "source": "analytics.transaction_fact + latest analytics.whale_score_snapshot",
        "excluded_markets": ["physical sports"],
        "excluded_sports_event_rows": excluded_sports_rows,
        "queried_event_rows": len(rows),
        "sequence_count": len(sequence_items),
        "items": sequence_items,
    }


def whale_event_sequences(
    session: Session,
    *,
    limit: int = 12,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    bucket_hours: int = DEFAULT_BUCKET_HOURS,
    trusted_only: bool = False,
    platform_name: str = DEFAULT_PLATFORM,
) -> dict[str, Any]:
    """Return semi-live whale event sequences grouped by market side."""
    clean_lookback_hours = min(max(int(lookback_hours), 1), MAX_LOOKBACK_HOURS)
    clean_bucket_hours = min(max(int(bucket_hours), 1), clean_lookback_hours)
    clean_limit = min(max(int(limit), 1), 50)
    clean_platform = (platform_name or DEFAULT_PLATFORM).strip().casefold()

    as_of_row = session.execute(AS_OF_SQL, {"platform_name": clean_platform}).mappings().first()
    as_of = as_of_row.get("as_of") if as_of_row else None
    if as_of is None:
        generated_at = datetime.now(timezone.utc)
        return {
            "available": False,
            "reason": f"No transaction data available for {clean_platform}.",
            "generated_at": generated_at.isoformat(),
            "as_of": None,
            "platform": clean_platform,
            "items": [],
        }

    score_batch = session.execute(
        LATEST_WHALE_BATCH_SQL,
        {"platform_name": clean_platform},
    ).mappings().first()
    if not score_batch:
        return {
            "available": False,
            "reason": f"No whale score snapshot is available for {clean_platform}.",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "as_of": as_of.isoformat(),
            "platform": clean_platform,
            "items": [],
        }

    cutoff_time = as_of - timedelta(hours=clean_lookback_hours)
    rows = session.execute(
        WHALE_EVENTS_SQL,
        {
            "platform_name": clean_platform,
            "score_snapshot_time": score_batch["snapshot_time"],
            "scoring_version": score_batch["scoring_version"],
            "cutoff_time": cutoff_time,
            "as_of": as_of,
            "trusted_only": bool(trusted_only),
            "max_rows": MAX_QUERY_ROWS,
        },
    ).mappings().all()

    grouped: dict[tuple[int, str], list[WhaleEventRow]] = {}
    excluded_sports_rows = 0
    for raw_row in rows:
        event = _row_from_mapping(raw_row)
        if _is_sports_market(event):
            excluded_sports_rows += 1
            continue
        side_label = _normalized_label(event.outcome_label)
        if not side_label:
            continue
        grouped.setdefault((event.market_contract_id, side_label), []).append(event)

    sequence_items = [
        _sequence_payload(events, as_of=as_of, lookback_hours=clean_lookback_hours, bucket_hours=clean_bucket_hours)
        for events in grouped.values()
        if events
    ]
    sequence_items.sort(key=lambda item: item.pop("_sort_key"), reverse=True)

    return {
        "available": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of": as_of.isoformat(),
        "platform": clean_platform,
        "score_snapshot_time": score_batch["snapshot_time"].isoformat(),
        "scoring_version": score_batch["scoring_version"],
        "lookback_hours": clean_lookback_hours,
        "bucket_hours": clean_bucket_hours,
        "trusted_only": bool(trusted_only),
        "semi_live": True,
        "cache_ttl_seconds": 30,
        "source": "analytics.transaction_fact + latest analytics.whale_score_snapshot",
        "excluded_markets": ["physical sports"],
        "excluded_sports_event_rows": excluded_sports_rows,
        "queried_event_rows": len(rows),
        "sequence_count": len(sequence_items),
        "items": sequence_items[:clean_limit],
        "feature_notes": [
            "Each item is a market-side sequence, not a final price prediction.",
            "Signal time is the latest whale entry when available, otherwise the latest whale event.",
            "These features are intended for the next trend-model training pass.",
        ],
    }
