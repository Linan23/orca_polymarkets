"""Evaluate actual-vs-ML trend similarity for whale movement predictions."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.jobs.export_ml_market_projection_example import (
    DEFAULT_COMPARISON_PATH,
    DEFAULT_DATASET_PATH,
    PREDICTION_WINDOWS,
    _clip_probability,
    _current_odds,
    _enrich_trend_features,
    _is_sports_market,
    _mae,
    _market_family_segment,
    _pair_group_key,
    _predict_rolling_rows,
    _rmse,
    _round,
    _safe_float,
    _selected_prediction_specs,
)
from data_platform.ml.market_baseline_model import (
    GROUP_KEY_COLUMN,
    REGIME_TRADE_COVERED,
    _filter_rows_by_regime,
    _load_training_rows,
    _research_focus_segment,
)


DEFAULT_OUTPUT_JSON_PATH = Path("data_platform/ml/ML_TREND_SIMILARITY_CURRENT_DB_ASOF.json")
DEFAULT_OUTPUT_MARKDOWN_PATH = Path("data_platform/ml/ML_TREND_SIMILARITY_CURRENT_DB_ASOF.md")
DEFAULT_DIRECTION_THRESHOLD = 0.005
DEFAULT_TRAJECTORY_SIMILARITY_ERROR = 0.05


def _pct(value: float) -> float:
    """Return a probability value as percentage points."""
    return _round(float(value) * 100.0, 4)


def _direction(delta: float, threshold: float) -> str:
    """Return a thresholded movement direction label."""
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "flat"


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    """Return a rounded ratio percentage."""
    denominator = float(denominator)
    if denominator <= 0:
        return 0.0
    return _round(float(numerator) / denominator * 100.0, 4)


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """Return Pearson correlation, or None when variance is unavailable."""
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    x_var = sum((value - x_mean) ** 2 for value in xs)
    y_var = sum((value - y_mean) ** 2 for value in ys)
    if x_var <= 0 or y_var <= 0:
        return None
    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True))
    return _round(covariance / ((x_var * y_var) ** 0.5), 6)


def _record_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """Return a stable prediction record key."""
    return (
        str(row[GROUP_KEY_COLUMN]),
        str(row["market_slug"]),
        str(row["side_label"]),
        str(row["observation_time"]),
    )


def _prediction_records(
    *,
    predictions: dict[tuple[str, str, str, str], dict[str, Any]],
    direction_threshold: float,
) -> list[dict[str, Any]]:
    """Flatten rolling predictions into actual-vs-predicted trend records."""
    records: list[dict[str, Any]] = []
    for item in predictions.values():
        row = item["row"]
        current_odds = _current_odds(row)
        for window_name, prediction in item["windows"].items():
            target_column = f"future_price_side_{window_name}"
            observed_column = f"future_price_observed_{window_name}"
            if _safe_float(row, observed_column) < 0.5:
                continue
            actual_odds = _safe_float(row, target_column)
            price_predicted_odds = _clip_probability(current_odds + float(prediction["price_delta"]))
            whale_predicted_odds = _clip_probability(current_odds + float(prediction["corrected_delta"]))
            actual_delta = actual_odds - current_odds
            price_delta = price_predicted_odds - current_odds
            whale_delta = whale_predicted_odds - current_odds
            actual_direction = _direction(actual_delta, direction_threshold)
            records.append(
                {
                    "row": row,
                    "key": _record_key(row),
                    "window": window_name,
                    "current_odds": current_odds,
                    "actual_odds": actual_odds,
                    "price_predicted_odds": price_predicted_odds,
                    "whale_predicted_odds": whale_predicted_odds,
                    "actual_delta": actual_delta,
                    "price_delta": price_delta,
                    "whale_delta": whale_delta,
                    "actual_direction": actual_direction,
                    "price_direction": _direction(price_delta, direction_threshold),
                    "whale_direction": _direction(whale_delta, direction_threshold),
                    "price_direction_matches_actual": _direction(price_delta, direction_threshold)
                    == actual_direction,
                    "whale_direction_matches_actual": _direction(whale_delta, direction_threshold)
                    == actual_direction,
                    "price_abs_error": abs(actual_odds - price_predicted_odds),
                    "whale_abs_error": abs(actual_odds - whale_predicted_odds),
                    "price_delta_abs_error": abs(actual_delta - price_delta),
                    "whale_delta_abs_error": abs(actual_delta - whale_delta),
                    "whale_error_improvement": abs(actual_odds - price_predicted_odds)
                    - abs(actual_odds - whale_predicted_odds),
                    "event_category": str(row.get("event_category") or "uncategorized"),
                    "market_family": _market_family_segment(row),
                    "research_focus": _research_focus_segment(row),
                    "pair_key": "|".join(_pair_group_key(row)),
                    "estimator_profile": prediction.get("estimator_profile", ""),
                    "selected_config": prediction.get("selected_config", ""),
                }
            )
    return records


def _window_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return actual-vs-ML trend similarity metrics for one record group."""
    actual_odds = [float(record["actual_odds"]) for record in records]
    price_odds = [float(record["price_predicted_odds"]) for record in records]
    whale_odds = [float(record["whale_predicted_odds"]) for record in records]
    actual_deltas = [float(record["actual_delta"]) for record in records]
    price_deltas = [float(record["price_delta"]) for record in records]
    whale_deltas = [float(record["whale_delta"]) for record in records]
    actual_nonflat = [record for record in records if record["actual_direction"] != "flat"]
    whale_better = [record for record in records if float(record["whale_error_improvement"]) > 0]
    whale_similar_count = sum(
        1
        for record in records
        if bool(record["whale_direction_matches_actual"])
        and float(record["whale_abs_error"]) <= DEFAULT_TRAJECTORY_SIMILARITY_ERROR
    )
    actual_direction_counts = Counter(str(record["actual_direction"]) for record in records)
    return {
        "row_count": len(records),
        "condition_count": len({str(record["row"][GROUP_KEY_COLUMN]) for record in records}),
        "actual_direction_counts": dict(sorted(actual_direction_counts.items())),
        "price_direction_match_pct": _safe_ratio(
            sum(1 for record in records if bool(record["price_direction_matches_actual"])),
            len(records),
        ),
        "whale_direction_match_pct": _safe_ratio(
            sum(1 for record in records if bool(record["whale_direction_matches_actual"])),
            len(records),
        ),
        "price_direction_match_nonflat_pct": _safe_ratio(
            sum(1 for record in actual_nonflat if bool(record["price_direction_matches_actual"])),
            len(actual_nonflat),
        ),
        "whale_direction_match_nonflat_pct": _safe_ratio(
            sum(1 for record in actual_nonflat if bool(record["whale_direction_matches_actual"])),
            len(actual_nonflat),
        ),
        "actual_vs_price_delta_correlation": _pearson(actual_deltas, price_deltas),
        "actual_vs_whale_delta_correlation": _pearson(actual_deltas, whale_deltas),
        "price_rmse_pts": _pct(_rmse(actual_odds, price_odds)),
        "whale_rmse_pts": _pct(_rmse(actual_odds, whale_odds)),
        "whale_rmse_delta_vs_price_pts": _pct(_rmse(actual_odds, whale_odds) - _rmse(actual_odds, price_odds)),
        "price_mae_pts": _pct(_mae(actual_odds, price_odds)),
        "whale_mae_pts": _pct(_mae(actual_odds, whale_odds)),
        "whale_mae_delta_vs_price_pts": _pct(_mae(actual_odds, whale_odds) - _mae(actual_odds, price_odds)),
        "price_delta_rmse_pts": _pct(_rmse(actual_deltas, price_deltas)),
        "whale_delta_rmse_pts": _pct(_rmse(actual_deltas, whale_deltas)),
        "whale_delta_rmse_delta_vs_price_pts": _pct(
            _rmse(actual_deltas, whale_deltas) - _rmse(actual_deltas, price_deltas)
        ),
        "whale_better_abs_error_pct": _safe_ratio(len(whale_better), len(records)),
        "whale_direction_and_error_similar_pct": _safe_ratio(whale_similar_count, len(records)),
    }


def _paired_trajectory_records(
    records: list[dict[str, Any]],
    *,
    direction_threshold: float,
    trajectory_similarity_error: float,
) -> list[dict[str, Any]]:
    """Return paired 12h/24h trajectory-shape records."""
    by_key: dict[tuple[str, str, str, str], dict[str, dict[str, Any]]] = {}
    for record in records:
        by_key.setdefault(record["key"], {})[str(record["window"])] = record

    paired: list[dict[str, Any]] = []
    for key, windows in by_key.items():
        if "12h" not in windows or "24h" not in windows:
            continue
        first = windows["12h"]
        second = windows["24h"]
        actual_slope = float(second["actual_odds"]) - float(first["actual_odds"])
        price_slope = float(second["price_predicted_odds"]) - float(first["price_predicted_odds"])
        whale_slope = float(second["whale_predicted_odds"]) - float(first["whale_predicted_odds"])
        price_trajectory_mae = (
            abs(float(first["actual_odds"]) - float(first["price_predicted_odds"]))
            + abs(float(second["actual_odds"]) - float(second["price_predicted_odds"]))
        ) / 2.0
        whale_trajectory_mae = (
            abs(float(first["actual_odds"]) - float(first["whale_predicted_odds"]))
            + abs(float(second["actual_odds"]) - float(second["whale_predicted_odds"]))
        ) / 2.0
        actual_slope_direction = _direction(actual_slope, direction_threshold)
        price_slope_direction = _direction(price_slope, direction_threshold)
        whale_slope_direction = _direction(whale_slope, direction_threshold)
        paired.append(
            {
                "key": key,
                "row": first["row"],
                "current_odds": first["current_odds"],
                "actual_12h_odds": first["actual_odds"],
                "actual_24h_odds": second["actual_odds"],
                "price_12h_predicted_odds": first["price_predicted_odds"],
                "price_24h_predicted_odds": second["price_predicted_odds"],
                "whale_12h_predicted_odds": first["whale_predicted_odds"],
                "whale_24h_predicted_odds": second["whale_predicted_odds"],
                "actual_slope": actual_slope,
                "price_slope": price_slope,
                "whale_slope": whale_slope,
                "actual_slope_direction": actual_slope_direction,
                "price_slope_direction": price_slope_direction,
                "whale_slope_direction": whale_slope_direction,
                "price_slope_matches_actual": price_slope_direction == actual_slope_direction,
                "whale_slope_matches_actual": whale_slope_direction == actual_slope_direction,
                "price_trajectory_mae": price_trajectory_mae,
                "whale_trajectory_mae": whale_trajectory_mae,
                "whale_trajectory_improvement": price_trajectory_mae - whale_trajectory_mae,
                "whale_trajectory_similar": whale_slope_direction == actual_slope_direction
                and whale_trajectory_mae <= trajectory_similarity_error,
                "event_category": first["event_category"],
                "market_family": first["market_family"],
                "research_focus": first["research_focus"],
                "market_slug": str(first["row"]["market_slug"]),
                "question": str(first["row"]["question"]),
                "side_label": str(first["row"]["side_label"]),
                "observation_time": str(first["row"]["observation_time"]),
            }
        )
    return paired


def _trajectory_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return 12h-to-24h trend-shape similarity metrics."""
    actual_slopes = [float(record["actual_slope"]) for record in records]
    price_slopes = [float(record["price_slope"]) for record in records]
    whale_slopes = [float(record["whale_slope"]) for record in records]
    price_maes = [float(record["price_trajectory_mae"]) for record in records]
    whale_maes = [float(record["whale_trajectory_mae"]) for record in records]
    actual_nonflat = [record for record in records if record["actual_slope_direction"] != "flat"]
    return {
        "paired_row_count": len(records),
        "condition_count": len({str(record["row"][GROUP_KEY_COLUMN]) for record in records}),
        "actual_slope_direction_counts": dict(
            sorted(Counter(str(record["actual_slope_direction"]) for record in records).items())
        ),
        "price_slope_direction_match_pct": _safe_ratio(
            sum(1 for record in records if bool(record["price_slope_matches_actual"])),
            len(records),
        ),
        "whale_slope_direction_match_pct": _safe_ratio(
            sum(1 for record in records if bool(record["whale_slope_matches_actual"])),
            len(records),
        ),
        "price_slope_direction_match_nonflat_pct": _safe_ratio(
            sum(1 for record in actual_nonflat if bool(record["price_slope_matches_actual"])),
            len(actual_nonflat),
        ),
        "whale_slope_direction_match_nonflat_pct": _safe_ratio(
            sum(1 for record in actual_nonflat if bool(record["whale_slope_matches_actual"])),
            len(actual_nonflat),
        ),
        "actual_vs_price_slope_correlation": _pearson(actual_slopes, price_slopes),
        "actual_vs_whale_slope_correlation": _pearson(actual_slopes, whale_slopes),
        "price_trajectory_mae_pts": _pct(sum(price_maes) / len(price_maes)) if price_maes else 0.0,
        "whale_trajectory_mae_pts": _pct(sum(whale_maes) / len(whale_maes)) if whale_maes else 0.0,
        "whale_trajectory_mae_delta_vs_price_pts": _pct(
            (sum(whale_maes) / len(whale_maes)) - (sum(price_maes) / len(price_maes))
        )
        if whale_maes and price_maes
        else 0.0,
        "whale_better_trajectory_pct": _safe_ratio(
            sum(1 for record in records if float(record["whale_trajectory_improvement"]) > 0),
            len(records),
        ),
        "whale_trajectory_similar_pct": _safe_ratio(
            sum(1 for record in records if bool(record["whale_trajectory_similar"])),
            len(records),
        ),
    }


def _group_records(records: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    """Group records by a string key."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    return grouped


def _group_summaries(records: list[dict[str, Any]], *, key: str, min_rows: int) -> list[dict[str, Any]]:
    """Return compact grouped trend summaries."""
    rows: list[dict[str, Any]] = []
    for value, group_records in _group_records(records, key).items():
        if len(group_records) < min_rows:
            continue
        summary = _window_summary(group_records)
        rows.append({"group": value, **summary})
    rows.sort(key=lambda item: (-int(item["row_count"]), str(item["group"])))
    return rows


def _trajectory_group_summaries(records: list[dict[str, Any]], *, key: str, min_pairs: int) -> list[dict[str, Any]]:
    """Return compact grouped trajectory summaries."""
    rows: list[dict[str, Any]] = []
    for value, group_records in _group_records(records, key).items():
        if len(group_records) < min_pairs:
            continue
        summary = _trajectory_summary(group_records)
        rows.append({"group": value, **summary})
    rows.sort(key=lambda item: (-int(item["paired_row_count"]), str(item["group"])))
    return rows


def _case_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Return a compact trajectory case."""
    return {
        "market_slug": record["market_slug"],
        "question": record["question"],
        "side_label": record["side_label"],
        "observation_time": record["observation_time"],
        "event_category": record["event_category"],
        "market_family": record["market_family"],
        "research_focus": record["research_focus"],
        "actual_slope_pts": _pct(record["actual_slope"]),
        "price_slope_pts": _pct(record["price_slope"]),
        "whale_slope_pts": _pct(record["whale_slope"]),
        "price_trajectory_mae_pts": _pct(record["price_trajectory_mae"]),
        "whale_trajectory_mae_pts": _pct(record["whale_trajectory_mae"]),
        "whale_trajectory_improvement_pts": _pct(record["whale_trajectory_improvement"]),
        "actual_slope_direction": record["actual_slope_direction"],
        "price_slope_direction": record["price_slope_direction"],
        "whale_slope_direction": record["whale_slope_direction"],
        "whale_trajectory_similar": bool(record["whale_trajectory_similar"]),
        "trend_points": [
            {
                "hour": 0,
                "actual_odds_pct": _pct(record["current_odds"]),
                "price_only_predicted_odds_pct": _pct(record["current_odds"]),
                "whale_adjusted_predicted_odds_pct": _pct(record["current_odds"]),
            },
            {
                "hour": 12,
                "actual_odds_pct": _pct(record["actual_12h_odds"]),
                "price_only_predicted_odds_pct": _pct(record["price_12h_predicted_odds"]),
                "whale_adjusted_predicted_odds_pct": _pct(record["whale_12h_predicted_odds"]),
            },
            {
                "hour": 24,
                "actual_odds_pct": _pct(record["actual_24h_odds"]),
                "price_only_predicted_odds_pct": _pct(record["price_24h_predicted_odds"]),
                "whale_adjusted_predicted_odds_pct": _pct(record["whale_24h_predicted_odds"]),
            },
        ],
    }


def _selected_cases(trajectory_records: list[dict[str, Any]], limit: int) -> dict[str, list[dict[str, Any]]]:
    """Return best and worst actual-vs-whale trend examples."""
    best = sorted(
        trajectory_records,
        key=lambda record: float(record["whale_trajectory_improvement"]),
        reverse=True,
    )[:limit]
    worst = sorted(
        trajectory_records,
        key=lambda record: float(record["whale_trajectory_improvement"]),
    )[:limit]
    similar = [
        record
        for record in sorted(
            trajectory_records,
            key=lambda record: (
                bool(record["whale_trajectory_similar"]),
                float(record["whale_trajectory_improvement"]),
            ),
            reverse=True,
        )
        if bool(record["whale_trajectory_similar"])
    ][:limit]
    by_category: dict[str, list[dict[str, Any]]] = {}
    for record in trajectory_records:
        by_category.setdefault(str(record["event_category"]), []).append(record)
    diverse: list[dict[str, Any]] = []
    for category, records in sorted(by_category.items()):
        selected = sorted(
            records,
            key=lambda record: (
                str(record["actual_slope_direction"]) != "flat",
                bool(record["whale_trajectory_similar"]),
                float(record["whale_trajectory_improvement"]),
            ),
            reverse=True,
        )[0]
        diverse.append(selected)
    diverse.sort(
        key=lambda record: (
            str(record["actual_slope_direction"]) == "flat",
            -abs(float(record["actual_slope"])),
            str(record["event_category"]),
        )
    )
    return {
        "best_whale_trend_matches": [_case_payload(record) for record in best],
        "largest_whale_trend_misses": [_case_payload(record) for record in worst],
        "similar_whale_trajectories": [_case_payload(record) for record in similar],
        "diverse_market_examples": [_case_payload(record) for record in diverse[:limit]],
    }


def _similarity_note(summary: dict[str, Any]) -> str:
    """Return a short interpretation of aggregate trend similarity."""
    trajectory = summary["trajectory"]
    whale_match = float(trajectory["whale_slope_direction_match_pct"])
    price_match = float(trajectory["price_slope_direction_match_pct"])
    whale_mae_delta = float(trajectory["whale_trajectory_mae_delta_vs_price_pts"])
    if whale_match >= price_match and whale_mae_delta <= 0:
        return "Whale-adjusted ML trend is broadly similar to actual trend and improves over price-only on trajectory shape."
    if whale_match >= price_match:
        return "Whale-adjusted ML trend usually points in the same direction as actual trend, but magnitude error is mixed."
    if whale_mae_delta <= 0:
        return "Whale-adjusted ML improves magnitude error, but direction similarity is mixed."
    return "Whale-adjusted ML trend similarity is mixed and should not be treated as a reliable trend overlay yet."


def _write_markdown(payload: dict[str, Any], output_path: Path) -> None:
    """Write a concise markdown report."""
    lines = [
        "# ML Trend Similarity",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Dataset: `{payload['dataset_path']}`",
        f"Comparison: `{payload['comparison_path']}`",
        f"Regime: `{payload['regime']}`",
        "",
        "## Summary",
        "",
        payload["similarity_note"],
        "",
        "## Window Metrics",
        "",
        "| Window | Rows | Price Direction Match | Whale Direction Match | Whale Non-Flat Match | Price RMSE | Whale RMSE | RMSE Delta | Price Corr | Whale Corr |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for window_name, summary in payload["windows"].items():
        lines.append(
            "| {window} | {rows} | {price_match}% | {whale_match}% | {whale_nonflat}% | {price_rmse} | {whale_rmse} | {delta} | {price_corr} | {whale_corr} |".format(
                window=window_name,
                rows=summary["row_count"],
                price_match=summary["price_direction_match_pct"],
                whale_match=summary["whale_direction_match_pct"],
                whale_nonflat=summary["whale_direction_match_nonflat_pct"],
                price_rmse=summary["price_rmse_pts"],
                whale_rmse=summary["whale_rmse_pts"],
                delta=summary["whale_rmse_delta_vs_price_pts"],
                price_corr=summary["actual_vs_price_delta_correlation"],
                whale_corr=summary["actual_vs_whale_delta_correlation"],
            )
        )
    trajectory = payload["trajectory"]
    lines.extend(
        [
            "",
            "## 12h To 24h Trajectory",
            "",
            "| Pairs | Actual Slope Mix | Price Slope Match | Whale Slope Match | Whale Non-Flat Match | Price Trajectory MAE | Whale Trajectory MAE | MAE Delta | Similar Whale Trajectories |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            "| {pairs} | {slope_mix} | {price_match}% | {whale_match}% | {whale_nonflat}% | {price_mae} | {whale_mae} | {delta} | {similar}% |".format(
                pairs=trajectory["paired_row_count"],
                slope_mix=", ".join(
                    f"{key}: {value}" for key, value in trajectory["actual_slope_direction_counts"].items()
                ),
                price_match=trajectory["price_slope_direction_match_pct"],
                whale_match=trajectory["whale_slope_direction_match_pct"],
                whale_nonflat=trajectory["whale_slope_direction_match_nonflat_pct"],
                price_mae=trajectory["price_trajectory_mae_pts"],
                whale_mae=trajectory["whale_trajectory_mae_pts"],
                delta=trajectory["whale_trajectory_mae_delta_vs_price_pts"],
                similar=trajectory["whale_trajectory_similar_pct"],
            ),
            "",
            "## Category Trajectory Metrics",
            "",
            "| Category | Pairs | Actual Slope Mix | Whale Slope Match | Whale Non-Flat Match | Whale Trajectory MAE | MAE Delta | Similar Whale Trajectories |",
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["trajectory_by_category"]:
        lines.append(
            "| {group} | {pairs} | {slope_mix} | {match}% | {nonflat}% | {mae} | {delta} | {similar}% |".format(
                group=row["group"],
                pairs=row["paired_row_count"],
                slope_mix=", ".join(
                    f"{key}: {value}" for key, value in row["actual_slope_direction_counts"].items()
                ),
                match=row["whale_slope_direction_match_pct"],
                nonflat=row["whale_slope_direction_match_nonflat_pct"],
                mae=row["whale_trajectory_mae_pts"],
                delta=row["whale_trajectory_mae_delta_vs_price_pts"],
                similar=row["whale_trajectory_similar_pct"],
            )
        )
    lines.extend(["", "## Example Trend Matches", ""])
    for case in payload["cases"]["similar_whale_trajectories"][:5]:
        lines.append(
            "- `{market}` `{side}` at `{time}`: actual slope `{actual}`, whale slope `{whale}`, whale trajectory improvement `{improvement}` pts.".format(
                market=case["market_slug"],
                side=case["side_label"],
                time=case["observation_time"],
                actual=case["actual_slope_pts"],
                whale=case["whale_slope_pts"],
                improvement=case["whale_trajectory_improvement_pts"],
            )
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def evaluate_trend_similarity(
    *,
    dataset_path: Path,
    comparison_path: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    direction_threshold: float,
    trajectory_similarity_error: float,
    min_group_rows: int,
    case_limit: int,
) -> dict[str, Any]:
    """Evaluate trend similarity for current ML movement predictions."""
    selected_specs = _selected_prediction_specs(comparison_path)
    regime_rows = _filter_rows_by_regime(_load_training_rows(dataset_path), REGIME_TRADE_COVERED)
    rows = [row for row in regime_rows if not _is_sports_market(row)]
    excluded_physical_sports_rows = len(regime_rows) - len(rows)
    _enrich_trend_features(rows)
    predictions = _predict_rolling_rows(rows=rows, selected_specs=selected_specs)
    records = _prediction_records(predictions=predictions, direction_threshold=direction_threshold)
    records_by_window = {
        window_name: [record for record in records if record["window"] == window_name]
        for window_name in PREDICTION_WINDOWS
    }
    trajectory_records = _paired_trajectory_records(
        records,
        direction_threshold=direction_threshold,
        trajectory_similarity_error=trajectory_similarity_error,
    )
    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "comparison_path": str(comparison_path),
        "regime": REGIME_TRADE_COVERED,
        "excluded_physical_sports_rows": excluded_physical_sports_rows,
        "market_scope_note": "Physical sports are excluded; esports and video-game markets remain in scope.",
        "direction_threshold_pts": _pct(direction_threshold),
        "trajectory_similarity_error_pts": _pct(trajectory_similarity_error),
        "selected_model_specs": selected_specs,
        "record_count": len(records),
        "windows": {
            window_name: _window_summary(window_records)
            for window_name, window_records in records_by_window.items()
        },
        "window_by_category": {
            window_name: _group_summaries(window_records, key="event_category", min_rows=min_group_rows)
            for window_name, window_records in records_by_window.items()
        },
        "trajectory": _trajectory_summary(trajectory_records),
        "trajectory_by_category": _trajectory_group_summaries(
            trajectory_records,
            key="event_category",
            min_pairs=max(1, min_group_rows // 2),
        ),
        "trajectory_by_research_focus": _trajectory_group_summaries(
            trajectory_records,
            key="research_focus",
            min_pairs=max(1, min_group_rows // 2),
        ),
        "cases": _selected_cases(trajectory_records, case_limit),
    }
    payload["similarity_note"] = _similarity_note(payload)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(payload, output_markdown_path)
    return payload


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Evaluate actual-vs-ML whale trend similarity.")
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--comparison-path", default=str(DEFAULT_COMPARISON_PATH))
    parser.add_argument("--output-json-path", default=str(DEFAULT_OUTPUT_JSON_PATH))
    parser.add_argument("--output-markdown-path", default=str(DEFAULT_OUTPUT_MARKDOWN_PATH))
    parser.add_argument("--direction-threshold", type=float, default=DEFAULT_DIRECTION_THRESHOLD)
    parser.add_argument("--trajectory-similarity-error", type=float, default=DEFAULT_TRAJECTORY_SIMILARITY_ERROR)
    parser.add_argument("--min-group-rows", type=int, default=20)
    parser.add_argument("--case-limit", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    payload = evaluate_trend_similarity(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        output_json_path=Path(args.output_json_path),
        output_markdown_path=Path(args.output_markdown_path),
        direction_threshold=args.direction_threshold,
        trajectory_similarity_error=args.trajectory_similarity_error,
        min_group_rows=args.min_group_rows,
        case_limit=args.case_limit,
    )
    print(
        json.dumps(
            {
                "output_json_path": str(args.output_json_path),
                "output_markdown_path": str(args.output_markdown_path),
                "similarity_note": payload["similarity_note"],
                "windows": payload["windows"],
                "trajectory": payload["trajectory"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
