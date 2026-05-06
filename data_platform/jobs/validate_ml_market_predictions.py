"""Validate matured ML market prediction snapshots against actual Polymarket outcomes."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.models import MlMarketPredictionValidation
from data_platform.models.base import utc_now


DEFAULT_LIMIT = 1000
DEFAULT_TARGET_TOLERANCE_MINUTES = 90
VALIDATION_MODEL_VERSION = "market_profile_prediction_validation_v1"


@dataclass(frozen=True)
class ActualOutcome:
    """Actual target-time probability for one predicted market side."""

    actual_future_odds_pct: float
    actual_source: str
    actual_source_detail: str
    actual_observed_at: datetime | None
    target_delay_seconds: int | None
    payload: dict[str, Any]


def _parse_as_of(value: str | None) -> datetime:
    """Return the scoring timestamp."""
    if not value:
        return utc_now()
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _normalize_label(value: Any) -> str:
    """Normalize outcome labels for source-to-prediction matching."""
    return "".join(character for character in str(value or "").casefold() if character.isalnum())


def _safe_float(value: Any) -> float | None:
    """Return a finite float for DB numeric values."""
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _round(value: float | None, places: int = 6) -> float | None:
    """Round nullable floats for stable DB payloads."""
    return None if value is None else round(float(value), places)


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp a number to a closed interval."""
    return max(low, min(value, high))


def _direction_from_delta(delta: float | None) -> str | None:
    """Return the dashboard direction bucket for a percentage-point delta."""
    if delta is None:
        return None
    if delta > 0.25:
        return "up"
    if delta < -0.25:
        return "down"
    return "flat"


def _source_price(snapshot_row: dict[str, Any]) -> float | None:
    """Return the best 0-1 price from an orderbook snapshot row."""
    mid_price = _safe_float(snapshot_row.get("mid_price"))
    if mid_price is not None:
        return mid_price
    best_bid = _safe_float(snapshot_row.get("best_bid"))
    best_ask = _safe_float(snapshot_row.get("best_ask"))
    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2.0
    return best_bid if best_bid is not None else best_ask


def _side_price_pct(source_price: float, *, side_label: Any, outcome_a_label: Any) -> float:
    """Convert a contract source price into a side-relative percentage."""
    price_pct = _clamp(source_price * 100.0, 0.0, 100.0)
    if _normalize_label(side_label) == _normalize_label(outcome_a_label):
        return price_pct
    return 100.0 - price_pct


def _table_exists(session: Session, schema: str, table_name: str) -> bool:
    """Return whether a database table exists."""
    row = session.execute(
        text("SELECT to_regclass(:qualified_name) IS NOT NULL AS table_exists"),
        {"qualified_name": f"{schema}.{table_name}"},
    ).mappings().first()
    return bool(row and row.get("table_exists"))


def _candidate_rows(
    session: Session,
    *,
    platform_name: str,
    as_of: datetime,
    limit: int,
    revalidate: bool,
) -> list[dict[str, Any]]:
    """Return prediction snapshots whose 12h/24h target has matured."""
    validation_filter = ""
    if not revalidate:
        validation_filter = """
          AND (
            v.ml_market_prediction_validation_id IS NULL
            OR v.validation_status IN ('missing_actual', 'awaiting_actual')
          )
        """
    limit_clause = "" if limit <= 0 else "LIMIT :limit"
    query = text(
        f"""
        SELECT
          s.ml_market_prediction_snapshot_id,
          s.platform_id,
          s.market_contract_id,
          s.market_slug,
          s.side_label,
          s.prediction_window_hours,
          s.observation_time,
          s.prediction_target_time,
          s.prediction_generated_at,
          s.current_odds_pct,
          s.predicted_future_odds_pct,
          s.predicted_delta_pts,
          s.prediction_payload,
          mc.outcome_a_label,
          mc.outcome_b_label,
          mc.condition_ref,
          mc.is_closed,
          rc.resolved_condition_id,
          rc.winning_outcome_label,
          rc.resolved_at,
          rc.resolver_method,
          rc.confidence AS resolution_confidence
        FROM analytics.ml_market_prediction_snapshot s
        JOIN analytics.market_contract mc
          ON mc.market_contract_id = s.market_contract_id
        JOIN analytics.platform p
          ON p.platform_id = s.platform_id
        LEFT JOIN analytics.resolved_condition rc
          ON rc.platform_id = mc.platform_id
         AND rc.condition_ref = mc.condition_ref
        LEFT JOIN analytics.ml_market_prediction_validation v
          ON v.ml_market_prediction_snapshot_id = s.ml_market_prediction_snapshot_id
        WHERE p.platform_name = :platform_name
          AND s.prediction_target_time IS NOT NULL
          AND s.prediction_target_time <= :as_of
          AND s.predicted_future_odds_pct IS NOT NULL
          {validation_filter}
        ORDER BY s.prediction_target_time ASC, s.ml_market_prediction_snapshot_id ASC
        {limit_clause}
        """
    )
    params: dict[str, Any] = {
        "platform_name": platform_name,
        "as_of": as_of,
    }
    if limit > 0:
        params["limit"] = limit
    return [dict(row) for row in session.execute(query, params).mappings().all()]


def _actual_from_resolution(row: dict[str, Any]) -> ActualOutcome | None:
    """Return final 100/0 actual when the market resolved before the prediction target."""
    winning_label = row.get("winning_outcome_label")
    if not winning_label:
        return None
    target_time = row.get("prediction_target_time")
    resolved_at = row.get("resolved_at")
    if resolved_at is not None and target_time is not None and resolved_at > target_time:
        return None

    side_won = _normalize_label(row.get("side_label")) == _normalize_label(winning_label)
    actual_pct = 100.0 if side_won else 0.0
    delay_seconds = None
    if resolved_at is not None and target_time is not None:
        delay_seconds = int((resolved_at - target_time).total_seconds())
    return ActualOutcome(
        actual_future_odds_pct=actual_pct,
        actual_source="resolved_condition",
        actual_source_detail=f"resolved_condition:{row.get('resolved_condition_id')}",
        actual_observed_at=resolved_at,
        target_delay_seconds=delay_seconds,
        payload={
            "winning_outcome_label": winning_label,
            "resolver_method": row.get("resolver_method"),
            "resolution_confidence": _safe_float(row.get("resolution_confidence")),
            "resolved_at": resolved_at.isoformat() if isinstance(resolved_at, datetime) else None,
        },
    )


def _orderbook_tables(session: Session) -> list[str]:
    """Return available orderbook tables in newest-to-oldest storage order."""
    tables = []
    for table_name in ("orderbook_snapshot_part", "orderbook_snapshot"):
        if _table_exists(session, "analytics", table_name):
            tables.append(table_name)
    return tables


def _actual_from_orderbook(
    session: Session,
    row: dict[str, Any],
    *,
    tolerance_minutes: int,
) -> ActualOutcome | None:
    """Return the nearest side price around the target time from orderbook snapshots."""
    target_time = row.get("prediction_target_time")
    if target_time is None:
        return None
    table_names = _orderbook_tables(session)
    if not table_names:
        return None

    window_start = target_time - timedelta(minutes=tolerance_minutes)
    window_end = target_time + timedelta(minutes=tolerance_minutes)
    union_parts = [
        f"""
        SELECT
          '{table_name}' AS source_table,
          orderbook_snapshot_id,
          snapshot_time,
          mid_price,
          best_bid,
          best_ask
        FROM analytics.{table_name}
        WHERE market_contract_id = :market_contract_id
          AND snapshot_time >= :window_start
          AND snapshot_time <= :window_end
        """
        for table_name in table_names
    ]
    query = text(
        f"""
        SELECT *
        FROM (
          {" UNION ALL ".join(union_parts)}
        ) candidates
        ORDER BY ABS(EXTRACT(EPOCH FROM (snapshot_time - CAST(:target_time AS TIMESTAMPTZ)))) ASC,
                 snapshot_time DESC
        LIMIT 1
        """
    )
    snapshot = session.execute(
        query,
        {
            "market_contract_id": int(row["market_contract_id"]),
            "window_start": window_start,
            "window_end": window_end,
            "target_time": target_time,
        },
    ).mappings().first()
    if not snapshot:
        return None

    snapshot_row = dict(snapshot)
    source_price = _source_price(snapshot_row)
    if source_price is None:
        return None
    actual_pct = _side_price_pct(source_price, side_label=row.get("side_label"), outcome_a_label=row.get("outcome_a_label"))
    observed_at = snapshot_row.get("snapshot_time")
    delay_seconds = None
    if isinstance(observed_at, datetime):
        delay_seconds = int((observed_at - target_time).total_seconds())
    return ActualOutcome(
        actual_future_odds_pct=actual_pct,
        actual_source=str(snapshot_row["source_table"]),
        actual_source_detail=f"{snapshot_row['source_table']}:{snapshot_row.get('orderbook_snapshot_id')}",
        actual_observed_at=observed_at,
        target_delay_seconds=delay_seconds,
        payload={
            "source_price": _round(source_price),
            "mid_price": _round(_safe_float(snapshot_row.get("mid_price"))),
            "best_bid": _round(_safe_float(snapshot_row.get("best_bid"))),
            "best_ask": _round(_safe_float(snapshot_row.get("best_ask"))),
            "target_tolerance_minutes": tolerance_minutes,
        },
    )


def _actual_outcome(
    session: Session,
    row: dict[str, Any],
    *,
    tolerance_minutes: int,
) -> ActualOutcome | None:
    """Return the best actual target for a matured prediction."""
    return _actual_from_resolution(row) or _actual_from_orderbook(
        session,
        row,
        tolerance_minutes=tolerance_minutes,
    )


def _validation_row(
    session: Session,
    row: dict[str, Any],
    *,
    tolerance_minutes: int,
) -> dict[str, Any]:
    """Return an upsert-ready validation row for one prediction snapshot."""
    now = utc_now()
    actual = _actual_outcome(session, row, tolerance_minutes=tolerance_minutes)
    current_pct = _safe_float(row.get("current_odds_pct"))
    predicted_future_pct = _safe_float(row.get("predicted_future_odds_pct"))
    predicted_delta = _safe_float(row.get("predicted_delta_pts"))
    if predicted_delta is None and predicted_future_pct is not None and current_pct is not None:
        predicted_delta = predicted_future_pct - current_pct
    predicted_direction = _direction_from_delta(predicted_delta)
    prediction_payload = row.get("prediction_payload") if isinstance(row.get("prediction_payload"), dict) else {}

    actual_future_pct = actual.actual_future_odds_pct if actual else None
    actual_delta = None if actual_future_pct is None or current_pct is None else actual_future_pct - current_pct
    actual_direction = _direction_from_delta(actual_delta)
    signed_error = None
    absolute_error = None
    squared_error = None
    direction_match = None
    validation_status = "missing_actual"
    if predicted_future_pct is not None and actual_future_pct is not None:
        signed_error = predicted_future_pct - actual_future_pct
        absolute_error = abs(signed_error)
        squared_error = signed_error * signed_error
        direction_match = predicted_direction == actual_direction
        validation_status = "validated"

    validation_payload = {
        "validation_model_version": VALIDATION_MODEL_VERSION,
        "market_contract_id": row.get("market_contract_id"),
        "condition_ref": row.get("condition_ref"),
        "outcome_a_label": row.get("outcome_a_label"),
        "outcome_b_label": row.get("outcome_b_label"),
        "prediction_status": prediction_payload.get("prediction_status"),
        "prediction_source": prediction_payload.get("prediction_source"),
        "direction_signal_tier": prediction_payload.get("direction_signal_tier"),
        "display_tier": prediction_payload.get("display_tier"),
        "whale_anchor": prediction_payload.get("whale_anchor") or {},
        "actual": actual.payload if actual else {"reason": "no resolved outcome or target-time orderbook snapshot"},
    }

    return {
        "ml_market_prediction_snapshot_id": int(row["ml_market_prediction_snapshot_id"]),
        "platform_id": int(row["platform_id"]),
        "market_contract_id": int(row["market_contract_id"]),
        "market_slug": str(row["market_slug"]),
        "side_label": str(row["side_label"]),
        "prediction_window_hours": int(row["prediction_window_hours"]),
        "observation_time": row["observation_time"],
        "prediction_target_time": row["prediction_target_time"],
        "prediction_generated_at": row["prediction_generated_at"],
        "current_odds_pct": _round(current_pct),
        "predicted_future_odds_pct": _round(predicted_future_pct),
        "predicted_delta_pts": _round(predicted_delta),
        "actual_future_odds_pct": _round(actual_future_pct),
        "actual_delta_pts": _round(actual_delta),
        "signed_error_pts": _round(signed_error),
        "absolute_error_pts": _round(absolute_error),
        "squared_error_pts": _round(squared_error),
        "predicted_direction": predicted_direction,
        "actual_direction": actual_direction,
        "direction_match": direction_match,
        "validation_status": validation_status,
        "actual_source": actual.actual_source if actual else None,
        "actual_source_detail": actual.actual_source_detail if actual else None,
        "actual_observed_at": actual.actual_observed_at if actual else None,
        "target_delay_seconds": actual.target_delay_seconds if actual else None,
        "validation_payload": validation_payload,
        "validated_at": now,
        "created_at": now,
        "updated_at": now,
    }


def _upsert_validations(session: Session, rows: list[dict[str, Any]]) -> None:
    """Persist validation rows idempotently."""
    if not rows:
        return
    statement = pg_insert(MlMarketPredictionValidation).values(rows)
    statement = statement.on_conflict_do_update(
        constraint="uq_ml_market_prediction_validation_snapshot",
        set_={
            "actual_future_odds_pct": statement.excluded.actual_future_odds_pct,
            "actual_delta_pts": statement.excluded.actual_delta_pts,
            "signed_error_pts": statement.excluded.signed_error_pts,
            "absolute_error_pts": statement.excluded.absolute_error_pts,
            "squared_error_pts": statement.excluded.squared_error_pts,
            "predicted_direction": statement.excluded.predicted_direction,
            "actual_direction": statement.excluded.actual_direction,
            "direction_match": statement.excluded.direction_match,
            "validation_status": statement.excluded.validation_status,
            "actual_source": statement.excluded.actual_source,
            "actual_source_detail": statement.excluded.actual_source_detail,
            "actual_observed_at": statement.excluded.actual_observed_at,
            "target_delay_seconds": statement.excluded.target_delay_seconds,
            "validation_payload": statement.excluded.validation_payload,
            "validated_at": statement.excluded.validated_at,
            "updated_at": statement.excluded.updated_at,
        },
    )
    session.execute(statement)


def _empty_metric_summary() -> dict[str, Any]:
    """Return an empty metrics object."""
    return {
        "validated_count": 0,
        "missing_actual_count": 0,
        "mae_pts": None,
        "rmse_pts": None,
        "direction_accuracy": None,
    }


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return compact validation metrics from rows processed in this run."""
    by_window: dict[str, dict[str, Any]] = {}
    by_source: dict[str, int] = {}
    validated_rows = [row for row in rows if row.get("validation_status") == "validated"]
    for row in rows:
        window_name = f"{int(row['prediction_window_hours'])}h"
        bucket = by_window.setdefault(window_name, _empty_metric_summary())
        if row.get("validation_status") == "validated":
            bucket["validated_count"] += 1
        else:
            bucket["missing_actual_count"] += 1
        source = str(row.get("actual_source") or row.get("validation_status") or "unknown")
        by_source[source] = by_source.get(source, 0) + 1

    for window_name, bucket in by_window.items():
        window_rows = [
            row
            for row in rows
            if f"{int(row['prediction_window_hours'])}h" == window_name and row.get("validation_status") == "validated"
        ]
        if not window_rows:
            continue
        abs_errors = [float(row["absolute_error_pts"]) for row in window_rows if row.get("absolute_error_pts") is not None]
        squared_errors = [float(row["squared_error_pts"]) for row in window_rows if row.get("squared_error_pts") is not None]
        direction_values = [bool(row["direction_match"]) for row in window_rows if row.get("direction_match") is not None]
        bucket["mae_pts"] = round(sum(abs_errors) / len(abs_errors), 6) if abs_errors else None
        bucket["rmse_pts"] = round(math.sqrt(sum(squared_errors) / len(squared_errors)), 6) if squared_errors else None
        bucket["direction_accuracy"] = (
            round(sum(1 for value in direction_values if value) / len(direction_values), 6)
            if direction_values
            else None
        )

    return {
        "candidate_count": len(rows),
        "validated_count": len(validated_rows),
        "missing_actual_count": len(rows) - len(validated_rows),
        "by_window": by_window,
        "by_source": by_source,
    }


def validate_predictions(
    session: Session,
    *,
    platform_name: str,
    as_of: datetime,
    limit: int,
    target_tolerance_minutes: int,
    create_table: bool,
    revalidate: bool,
) -> dict[str, Any]:
    """Validate matured prediction snapshots and persist actual-vs-predicted errors."""
    if create_table:
        MlMarketPredictionValidation.__table__.create(bind=session.get_bind(), checkfirst=True)
    if not _table_exists(session, "analytics", "ml_market_prediction_validation"):
        return {
            "ok": False,
            "reason": "ml_market_prediction_validation_table_missing",
            "hint": "Run Alembic migration 20260506_1000 or pass --create-table for local testing.",
        }

    candidates = _candidate_rows(
        session,
        platform_name=platform_name,
        as_of=as_of,
        limit=limit,
        revalidate=revalidate,
    )
    validation_rows = [
        _validation_row(session, row, tolerance_minutes=target_tolerance_minutes)
        for row in candidates
    ]
    _upsert_validations(session, validation_rows)
    return {
        "ok": True,
        "as_of": as_of.isoformat(),
        "platform": platform_name,
        "target_tolerance_minutes": target_tolerance_minutes,
        "limit": limit,
        "revalidate": revalidate,
        **_summary(validation_rows),
    }


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--platform-name", default=os.getenv("ML_PREDICTION_VALIDATION_PLATFORM", "polymarket"))
    parser.add_argument("--as-of", default="", help="Validation timestamp. Defaults to now.")
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_LIMIT", str(DEFAULT_LIMIT))),
        help="Maximum matured prediction snapshots to validate. Use 0 for no cap.",
    )
    parser.add_argument(
        "--target-tolerance-minutes",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_TARGET_TOLERANCE_MINUTES", str(DEFAULT_TARGET_TOLERANCE_MINUTES))),
        help="Nearest orderbook snapshot tolerance around the 12h/24h target time.",
    )
    parser.add_argument("--revalidate", action="store_true", help="Rescore already validated snapshots.")
    parser.add_argument("--create-table", action="store_true", help="Create the validation table when running locally.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    as_of = _parse_as_of(args.as_of)
    with session_scope(args.database_url or None) as session:
        summary = validate_predictions(
            session,
            platform_name=str(args.platform_name),
            as_of=as_of,
            limit=int(args.limit),
            target_tolerance_minutes=max(int(args.target_tolerance_minutes), 0),
            create_table=bool(args.create_table),
            revalidate=bool(args.revalidate),
        )
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
