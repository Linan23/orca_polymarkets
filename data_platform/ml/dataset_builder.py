"""Build the first model-ready dataset from resolved Polymarket user/market history."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from data_platform.ingest.store import UNKNOWN_USER_EXTERNAL_REF
from data_platform.services.whale_scoring import load_resolved_market_outcomes


DATASET_VERSION = "ml_user_market_v1"
DEFAULT_OUTPUT_DIR = Path("data_platform/runtime/ml")
DEFAULT_DATASET_PATH = DEFAULT_OUTPUT_DIR / "resolved_user_market_features.csv"
DEFAULT_METADATA_PATH = DEFAULT_OUTPUT_DIR / "resolved_user_market_features.metadata.json"
TARGET_COLUMN = "label_positive_realized_pnl"

MODEL_FEATURE_COLUMNS = (
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "distinct_trade_days",
    "distinct_outcomes_traded",
    "total_buy_shares",
    "total_sell_shares",
    "net_shares",
    "total_buy_notional",
    "total_sell_notional",
    "net_cash_outflow",
    "weighted_avg_buy_price",
    "weighted_avg_sell_price",
    "min_trade_price",
    "max_trade_price",
    "price_range",
    "primary_buy_shares",
    "primary_sell_shares",
    "primary_net_shares",
    "secondary_buy_shares",
    "secondary_sell_shares",
    "secondary_net_shares",
    "primary_buy_notional_share",
    "primary_buy_shares_share",
    "first_trade_hours_before_end",
    "last_trade_hours_before_end",
    "trading_span_hours",
    "market_volume",
    "market_liquidity",
    "market_duration_hours",
)

IDENTIFIER_COLUMNS = (
    "dataset_version",
    "user_id",
    "external_user_ref",
    "event_id",
    "event_title",
    "event_slug",
    "market_contract_id",
    "market_slug",
    "question",
    "condition_ref",
    "market_start_time",
    "market_end_time",
)

AUDIT_COLUMNS = (
    "primary_outcome_label",
    "secondary_outcome_label",
    "winning_outcome_label",
    "label_backed_winner",
    "realized_pnl",
    "realized_roi",
    "label_positive_realized_pnl",
)

LEAKAGE_COLUMNS = (
    "winning_outcome_label",
    "label_backed_winner",
    "realized_pnl",
    "realized_roi",
    "label_positive_realized_pnl",
)

CSV_COLUMNS = IDENTIFIER_COLUMNS + MODEL_FEATURE_COLUMNS + AUDIT_COLUMNS


TRANSACTION_EXPORT_SQL = text(
    """
    SELECT
      ua.user_id,
      ua.external_user_ref,
      me.event_id,
      me.title AS event_title,
      me.slug AS event_slug,
      mc.market_contract_id,
      mc.market_slug,
      mc.question,
      mc.condition_ref,
      COALESCE(mc.start_time, me.start_time) AS market_start_time,
      COALESCE(mc.end_time, me.end_time, me.closed_time) AS market_end_time,
      COALESCE(mc.volume, me.volume, 0) AS market_volume,
      COALESCE(mc.liquidity, me.liquidity, 0) AS market_liquidity,
      tf.outcome_label,
      tf.side,
      tf.transaction_time,
      COALESCE(tf.shares, 0) AS shares,
      COALESCE(tf.notional_value, 0) AS notional_value,
      COALESCE(tf.price, 0) AS price
    FROM analytics.transaction_fact tf
    JOIN analytics.user_account ua
      ON ua.user_id = tf.user_id
    JOIN analytics.market_contract mc
      ON mc.market_contract_id = tf.market_contract_id
    JOIN analytics.market_event me
      ON me.event_id = tf.event_id
    JOIN analytics.platform p
      ON p.platform_id = tf.platform_id
    WHERE p.platform_name = 'polymarket'
      AND ua.external_user_ref <> :unknown_user_ref
      AND mc.condition_ref IS NOT NULL
      AND tf.outcome_label IS NOT NULL
      AND tf.side IN ('buy', 'sell')
    ORDER BY ua.user_id, mc.condition_ref, tf.transaction_time, tf.transaction_id
    """
)


@dataclass
class OutcomeAccumulator:
    """Aggregate transaction metrics for one outcome inside one user/market row."""

    buy_trade_count: int = 0
    sell_trade_count: int = 0
    bought_shares: float = 0.0
    sold_shares: float = 0.0
    buy_notional: float = 0.0
    sell_notional: float = 0.0

    @property
    def net_shares(self) -> float:
        """Return non-negative net shares under the conservative PnL assumption."""
        return max(self.bought_shares - self.sold_shares, 0.0)

    @property
    def avg_buy_price(self) -> float:
        """Return weighted average buy price for this outcome."""
        if self.bought_shares <= 0:
            return 0.0
        return self.buy_notional / self.bought_shares

    @property
    def avg_sell_price(self) -> float:
        """Return weighted average sell price for this outcome."""
        if self.sold_shares <= 0:
            return 0.0
        return self.sell_notional / self.sold_shares


def _normalized_label(value: str | None) -> str | None:
    """Normalize outcome labels so grouping and matching stay stable."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _numeric(value: Any) -> float:
    """Coerce nullable database numeric values into floats."""
    if value is None:
        return 0.0
    return float(value)


def _iso(value: datetime | None) -> str:
    """Serialize optional datetimes as ISO-8601 strings."""
    if value is None:
        return ""
    return value.isoformat()


def _hours_between(later: datetime | None, earlier: datetime | None) -> float:
    """Return a non-negative hour delta between two timestamps."""
    if later is None or earlier is None:
        return 0.0
    delta = (later - earlier).total_seconds() / 3600.0
    return round(max(delta, 0.0), 6)


def build_resolved_user_market_dataset(
    session: Session,
    *,
    start_time: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build resolved Polymarket user/market observations suitable for first-pass ML."""
    resolved_outcomes = load_resolved_market_outcomes(session)
    raw_rows = session.execute(
        TRANSACTION_EXPORT_SQL,
        {"unknown_user_ref": UNKNOWN_USER_EXTERNAL_REF},
    ).mappings()

    grouped: dict[tuple[int, str], dict[str, Any]] = {}
    for row in raw_rows:
        transaction_time = row["transaction_time"]
        if start_time is not None and transaction_time is not None and transaction_time < start_time:
            continue

        condition_ref = str(row["condition_ref"])
        user_id = int(row["user_id"])
        key = (user_id, condition_ref)
        bucket = grouped.setdefault(
            key,
            {
                "user_id": user_id,
                "external_user_ref": str(row["external_user_ref"]),
                "event_id": int(row["event_id"]),
                "event_title": str(row["event_title"]),
                "event_slug": row["event_slug"] or "",
                "market_contract_id": int(row["market_contract_id"]),
                "market_slug": row["market_slug"] or "",
                "question": str(row["question"]),
                "condition_ref": condition_ref,
                "market_start_time": row["market_start_time"],
                "market_end_time": row["market_end_time"],
                "market_volume": _numeric(row["market_volume"]),
                "market_liquidity": _numeric(row["market_liquidity"]),
                "trade_count": 0,
                "buy_trade_count": 0,
                "sell_trade_count": 0,
                "distinct_trade_days": set(),
                "first_trade_at": None,
                "last_trade_at": None,
                "min_trade_price": None,
                "max_trade_price": None,
                "outcomes": {},
            },
        )

        bucket["trade_count"] += 1
        bucket["distinct_trade_days"].add(transaction_time.date())
        if bucket["first_trade_at"] is None or transaction_time < bucket["first_trade_at"]:
            bucket["first_trade_at"] = transaction_time
        if bucket["last_trade_at"] is None or transaction_time > bucket["last_trade_at"]:
            bucket["last_trade_at"] = transaction_time

        price = _numeric(row["price"])
        if bucket["min_trade_price"] is None or price < bucket["min_trade_price"]:
            bucket["min_trade_price"] = price
        if bucket["max_trade_price"] is None or price > bucket["max_trade_price"]:
            bucket["max_trade_price"] = price

        outcome_label = _normalized_label(row["outcome_label"])
        if not outcome_label:
            continue
        outcome_bucket = bucket["outcomes"].setdefault(outcome_label, OutcomeAccumulator())
        shares = _numeric(row["shares"])
        notional_value = _numeric(row["notional_value"])
        if row["side"] == "buy":
            bucket["buy_trade_count"] += 1
            outcome_bucket.buy_trade_count += 1
            outcome_bucket.bought_shares += shares
            outcome_bucket.buy_notional += notional_value
        else:
            bucket["sell_trade_count"] += 1
            outcome_bucket.sell_trade_count += 1
            outcome_bucket.sold_shares += shares
            outcome_bucket.sell_notional += notional_value

    dataset_rows: list[dict[str, Any]] = []
    excluded_groups = {
        "unresolved_condition": 0,
        "oversold_condition": 0,
        "no_buy_notional": 0,
        "missing_outcome_activity": 0,
    }
    positive_label_count = 0

    for group in grouped.values():
        winning_outcome = resolved_outcomes.get(group["condition_ref"])
        if not winning_outcome:
            excluded_groups["unresolved_condition"] += 1
            continue

        outcome_map: dict[str, OutcomeAccumulator] = group["outcomes"]
        if not outcome_map:
            excluded_groups["missing_outcome_activity"] += 1
            continue

        if any(item.sold_shares > item.bought_shares + 1e-9 for item in outcome_map.values()):
            excluded_groups["oversold_condition"] += 1
            continue

        total_buy_shares = sum(item.bought_shares for item in outcome_map.values())
        total_sell_shares = sum(item.sold_shares for item in outcome_map.values())
        total_buy_notional = sum(item.buy_notional for item in outcome_map.values())
        total_sell_notional = sum(item.sell_notional for item in outcome_map.values())
        if total_buy_notional <= 0 or total_buy_shares <= 0:
            excluded_groups["no_buy_notional"] += 1
            continue

        outcome_items = sorted(
            outcome_map.items(),
            key=lambda item: (item[1].buy_notional, item[1].bought_shares, item[0]),
            reverse=True,
        )
        primary_outcome_label, primary_bucket = outcome_items[0]
        secondary_outcome_label = ""
        secondary_bucket = OutcomeAccumulator()
        if len(outcome_items) > 1:
            secondary_outcome_label, secondary_bucket = outcome_items[1]

        winning_bucket = outcome_map.get(winning_outcome, OutcomeAccumulator())
        condition_cash_flow = sum(item.sell_notional - item.buy_notional for item in outcome_map.values())
        condition_final_value = winning_bucket.net_shares
        realized_pnl = round(condition_cash_flow + condition_final_value, 8)
        realized_roi = round(realized_pnl / total_buy_notional, 8) if total_buy_notional > 0 else 0.0
        label_positive_realized_pnl = int(realized_pnl > 0)
        positive_label_count += label_positive_realized_pnl

        min_trade_price = _numeric(group["min_trade_price"])
        max_trade_price = _numeric(group["max_trade_price"])
        total_net_shares = max(total_buy_shares - total_sell_shares, 0.0)

        row = {
            "dataset_version": DATASET_VERSION,
            "user_id": group["user_id"],
            "external_user_ref": group["external_user_ref"],
            "event_id": group["event_id"],
            "event_title": group["event_title"],
            "event_slug": group["event_slug"],
            "market_contract_id": group["market_contract_id"],
            "market_slug": group["market_slug"],
            "question": group["question"],
            "condition_ref": group["condition_ref"],
            "market_start_time": _iso(group["market_start_time"]),
            "market_end_time": _iso(group["market_end_time"]),
            "trade_count": group["trade_count"],
            "buy_trade_count": group["buy_trade_count"],
            "sell_trade_count": group["sell_trade_count"],
            "distinct_trade_days": len(group["distinct_trade_days"]),
            "distinct_outcomes_traded": len(outcome_map),
            "total_buy_shares": round(total_buy_shares, 8),
            "total_sell_shares": round(total_sell_shares, 8),
            "net_shares": round(total_net_shares, 8),
            "total_buy_notional": round(total_buy_notional, 8),
            "total_sell_notional": round(total_sell_notional, 8),
            "net_cash_outflow": round(total_buy_notional - total_sell_notional, 8),
            "weighted_avg_buy_price": round(total_buy_notional / total_buy_shares, 8),
            "weighted_avg_sell_price": round(total_sell_notional / total_sell_shares, 8)
            if total_sell_shares > 0
            else 0.0,
            "min_trade_price": round(min_trade_price, 8),
            "max_trade_price": round(max_trade_price, 8),
            "price_range": round(max_trade_price - min_trade_price, 8),
            "primary_buy_shares": round(primary_bucket.bought_shares, 8),
            "primary_sell_shares": round(primary_bucket.sold_shares, 8),
            "primary_net_shares": round(primary_bucket.net_shares, 8),
            "secondary_buy_shares": round(secondary_bucket.bought_shares, 8),
            "secondary_sell_shares": round(secondary_bucket.sold_shares, 8),
            "secondary_net_shares": round(secondary_bucket.net_shares, 8),
            "primary_buy_notional_share": round(primary_bucket.buy_notional / total_buy_notional, 8),
            "primary_buy_shares_share": round(primary_bucket.bought_shares / total_buy_shares, 8),
            "first_trade_hours_before_end": _hours_between(group["market_end_time"], group["first_trade_at"]),
            "last_trade_hours_before_end": _hours_between(group["market_end_time"], group["last_trade_at"]),
            "trading_span_hours": _hours_between(group["last_trade_at"], group["first_trade_at"]),
            "market_volume": round(group["market_volume"], 8),
            "market_liquidity": round(group["market_liquidity"], 8),
            "market_duration_hours": _hours_between(group["market_end_time"], group["market_start_time"]),
            "primary_outcome_label": primary_outcome_label,
            "secondary_outcome_label": secondary_outcome_label,
            "winning_outcome_label": winning_outcome,
            "label_backed_winner": int(primary_outcome_label == winning_outcome),
            "realized_pnl": realized_pnl,
            "realized_roi": realized_roi,
            TARGET_COLUMN: label_positive_realized_pnl,
        }
        dataset_rows.append(row)

    metadata = {
        "dataset_version": DATASET_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(dataset_rows),
        "target_column": TARGET_COLUMN,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "identifier_columns": list(IDENTIFIER_COLUMNS),
        "audit_columns": list(AUDIT_COLUMNS),
        "leakage_columns": list(LEAKAGE_COLUMNS),
        "class_balance": {
            "positive_realized_pnl_rows": positive_label_count,
            "non_positive_realized_pnl_rows": len(dataset_rows) - positive_label_count,
        },
        "excluded_group_counts": excluded_groups,
        "assumptions": [
            "Polymarket only. Kalshi is excluded from user-level ML until trader identity improves.",
            "Rows represent resolved user x market observations built from normalized transaction_fact.",
            "Resolved outcomes reuse the conservative whale-scoring resolver.",
            "Markets are excluded when sold shares exceed captured buys for an outcome.",
            "Final market close price is intentionally excluded from the feature list to avoid post-resolution leakage.",
            "Leakage columns are exported for audit but should not be used as model features.",
        ],
        "timeframe_start": _iso(start_time),
    }
    return dataset_rows, metadata


def export_resolved_user_market_dataset(
    session: Session,
    *,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    start_time: datetime | None = None,
) -> dict[str, Any]:
    """Build and write the first model-ready dataset plus metadata."""
    dataset_path = dataset_path or DEFAULT_DATASET_PATH
    metadata_path = metadata_path or DEFAULT_METADATA_PATH
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    rows, metadata = build_resolved_user_market_dataset(session, start_time=start_time)
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
        "row_count": metadata["row_count"],
        "class_balance": metadata["class_balance"],
        "excluded_group_counts": metadata["excluded_group_counts"],
        "dataset_version": DATASET_VERSION,
    }
