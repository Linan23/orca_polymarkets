"""Evaluate residual ML predictions across multiple market categories."""

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
    _mae,
    _pair_group_key,
    _pair_side_consistency_summary,
    _pct,
    _predict_rolling_rows,
    _rmse,
    _round,
    _safe_float,
    _selected_prediction_specs,
    _trend_overlay_applies,
)
from data_platform.ml.market_baseline_model import (
    GROUP_KEY_COLUMN,
    REGIME_TRADE_COVERED,
    _filter_rows_by_regime,
    _load_training_rows,
    _market_family_segment,
    _research_focus_segment,
)


DEFAULT_OUTPUT_JSON_PATH = Path("data_platform/ml/CATEGORY_VALIDATION_RIDGE_TRADE_COVERED.json")
DEFAULT_OUTPUT_MARKDOWN_PATH = Path("data_platform/ml/CATEGORY_VALIDATION_RIDGE_TRADE_COVERED.md")
FEATURE_HEALTH_COLUMNS = (
    "whale_distinct_users",
    "whale_side_entry_trade_count",
    "whale_side_exit_trade_count",
    "whale_side_avg_holding_hours",
    "whale_side_realized_roi",
    "whale_side_recent_trade_count_1h",
    "whale_side_recent_trade_count_6h",
    "whale_side_recent_trade_count_12h",
    "whale_side_recent_trade_count_24h",
    "trusted_whale_distinct_users",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_realized_roi",
    "trusted_whale_side_recent_trade_count_1h",
    "trusted_whale_side_recent_trade_count_6h",
    "trusted_whale_side_recent_trade_count_12h",
    "trusted_whale_side_recent_trade_count_24h",
)


def _category_label(row: dict[str, Any]) -> str:
    """Return the primary market category label for dashboard/report grouping."""
    category = str(row.get("event_category") or "").strip().lower()
    if category:
        return category
    return _market_family_segment(row)


def _unique_condition_count(rows: list[dict[str, Any]]) -> int:
    """Return distinct market condition count."""
    return len({str(row[GROUP_KEY_COLUMN]) for row in rows})


def _pct_ratio(numerator: int, denominator: int) -> float:
    """Return a rounded percentage ratio."""
    return _round((float(numerator) / float(denominator) * 100.0) if denominator else 0.0, 4)


def _has_recent_activity(row: dict[str, Any], prefix: str) -> bool:
    """Return whether the row has any recent whale activity feature populated."""
    return any(
        _safe_float(row, column) > 0
        for column in FEATURE_HEALTH_COLUMNS
        if column.startswith(prefix)
    )


def _feature_health(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return per-category feature coverage diagnostics."""
    row_count = len(rows)
    counters = {
        "price_observed_rows": sum(1 for row in rows if _safe_float(row, "side_price_observed") > 0),
        "whale_activity_rows": sum(1 for row in rows if _safe_float(row, "whale_distinct_users") > 0),
        "whale_entry_rows": sum(1 for row in rows if _safe_float(row, "whale_side_entry_trade_count") > 0),
        "whale_exit_rows": sum(1 for row in rows if _safe_float(row, "whale_side_exit_trade_count") > 0),
        "whale_holding_rows": sum(1 for row in rows if _safe_float(row, "whale_side_avg_holding_hours") > 0),
        "whale_roi_rows": sum(1 for row in rows if _safe_float(row, "whale_side_realized_roi") != 0),
        "recent_whale_activity_rows": sum(
            1 for row in rows if _has_recent_activity(row, "whale_side_recent_trade_count")
        ),
        "trusted_whale_activity_rows": sum(1 for row in rows if _safe_float(row, "trusted_whale_distinct_users") > 0),
        "trusted_entry_rows": sum(1 for row in rows if _safe_float(row, "trusted_whale_side_entry_trade_count") > 0),
        "trusted_exit_rows": sum(1 for row in rows if _safe_float(row, "trusted_whale_side_exit_trade_count") > 0),
        "trusted_holding_rows": sum(1 for row in rows if _safe_float(row, "trusted_whale_side_avg_holding_hours") > 0),
        "trusted_roi_rows": sum(1 for row in rows if _safe_float(row, "trusted_whale_side_realized_roi") != 0),
        "recent_trusted_activity_rows": sum(
            1 for row in rows if _has_recent_activity(row, "trusted_whale_side_recent_trade_count")
        ),
    }
    return {
        "row_count": row_count,
        "condition_count": _unique_condition_count(rows),
        **counters,
        **{f"{key}_pct": _pct_ratio(value, row_count) for key, value in counters.items()},
    }


def _prediction_records(
    *,
    predictions: dict[tuple[str, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flatten rolling prediction output into row/window records."""
    records: list[dict[str, Any]] = []
    for item in predictions.values():
        row = item["row"]
        category = _category_label(row)
        for window_name, prediction in item["windows"].items():
            target_column = f"future_price_side_{window_name}"
            if _safe_float(row, f"future_price_observed_{window_name}") < 0.5:
                continue
            current_odds = _current_odds(row)
            actual = _safe_float(row, target_column)
            price_prediction = _clip_probability(current_odds + float(prediction["price_delta"]))
            whale_prediction = _clip_probability(current_odds + float(prediction["corrected_delta"]))
            price_error = abs(actual - price_prediction)
            whale_error = abs(actual - whale_prediction)
            records.append(
                {
                    "row": row,
                    "window": window_name,
                    "category": category,
                    "market_family": _market_family_segment(row),
                    "research_focus": _research_focus_segment(row),
                    "actual": actual,
                    "current_odds": current_odds,
                    "price_prediction": price_prediction,
                    "whale_prediction": whale_prediction,
                    "price_error": price_error,
                    "whale_error": whale_error,
                    "whale_error_improvement": price_error - whale_error,
                    "actual_movement": abs(actual - current_odds),
                    "overlay_applies": _trend_overlay_applies(row, window_name),
                    "fold_index": prediction["fold_index"],
                    "selected_config": prediction["selected_config"],
                    "estimator_profile": prediction.get("estimator_profile", "ridge"),
                    "estimator_type": prediction.get("estimator_type", "ridge"),
                }
            )
    return records


def _prediction_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return compact prediction metrics for a group of row/window records."""
    actuals = [float(record["actual"]) for record in records]
    price_predictions = [float(record["price_prediction"]) for record in records]
    whale_predictions = [float(record["whale_prediction"]) for record in records]
    price_rmse = _rmse(actuals, price_predictions)
    whale_rmse = _rmse(actuals, whale_predictions)
    price_mae = _mae(actuals, price_predictions)
    whale_mae = _mae(actuals, whale_predictions)
    return {
        "row_count": len(records),
        "condition_count": len({str(record["row"][GROUP_KEY_COLUMN]) for record in records}),
        "price_rmse_pts": _pct(price_rmse),
        "whale_adjusted_rmse_pts": _pct(whale_rmse),
        "whale_rmse_delta_vs_price_pts": _pct(whale_rmse - price_rmse),
        "price_mae_pts": _pct(price_mae),
        "whale_adjusted_mae_pts": _pct(whale_mae),
        "whale_mae_delta_vs_price_pts": _pct(whale_mae - price_mae),
        "mean_whale_error_improvement_pts": _pct(
            sum(float(record["whale_error_improvement"]) for record in records) / len(records)
        )
        if records
        else 0.0,
        "improving_row_pct": _pct_ratio(
            sum(1 for record in records if float(record["whale_error_improvement"]) > 0),
            len(records),
        ),
        "overlay_candidate_count": sum(1 for record in records if bool(record["overlay_applies"])),
    }


def _category_pair_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return pair consistency diagnostics for whale-adjusted category predictions."""
    pair_records = [
        {
            "row": record["row"],
            "actual": record["actual"],
            "whale_prediction": record["whale_prediction"],
        }
        for record in records
    ]
    return _pair_side_consistency_summary(pair_records, "whale_prediction")


def _case_reason(record: dict[str, Any]) -> str:
    """Return a compact reason for why a case was selected."""
    improvement = float(record["whale_error_improvement"])
    if improvement > 0:
        return "best_whale_improvement"
    if improvement < 0:
        return "largest_whale_regression"
    if bool(record["overlay_applies"]):
        return "trend_overlay_candidate"
    return "high_movement_case"


def _case_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Return a dashboard/report-safe case row."""
    row = record["row"]
    return {
        "reason": _case_reason(record),
        "category": record["category"],
        "market_family": record["market_family"],
        "research_focus": record["research_focus"],
        "window": record["window"],
        "market_slug": str(row["market_slug"]),
        "question": str(row["question"]),
        "side_label": str(row["side_label"]),
        "observation_time": str(row["observation_time"]),
        "hours_to_close": _round(_safe_float(row, "hours_to_close"), 2),
        "current_odds_pct": _pct(float(record["current_odds"])),
        "actual_future_odds_pct": _pct(float(record["actual"])),
        "price_only_predicted_odds_pct": _pct(float(record["price_prediction"])),
        "whale_adjusted_predicted_odds_pct": _pct(float(record["whale_prediction"])),
        "whale_error_improvement_pts": _pct(float(record["whale_error_improvement"])),
        "actual_movement_pts": _pct(float(record["actual_movement"])),
        "overlay_candidate": bool(record["overlay_applies"]),
        "fold_index": record["fold_index"],
        "estimator_profile": str(record.get("estimator_profile") or ""),
        "estimator_type": str(record.get("estimator_type") or ""),
        "whale_distinct_users": int(_safe_float(row, "whale_distinct_users")),
        "whale_entry_count": int(_safe_float(row, "whale_side_entry_trade_count")),
        "whale_exit_count": int(_safe_float(row, "whale_side_exit_trade_count")),
        "whale_avg_holding_hours": _round(_safe_float(row, "whale_side_avg_holding_hours"), 2),
        "whale_realized_roi_pct": _pct(_safe_float(row, "whale_side_realized_roi")),
        "trusted_whale_distinct_users": int(_safe_float(row, "trusted_whale_distinct_users")),
        "trusted_entry_count": int(_safe_float(row, "trusted_whale_side_entry_trade_count")),
        "trusted_exit_count": int(_safe_float(row, "trusted_whale_side_exit_trade_count")),
        "trusted_avg_holding_hours": _round(_safe_float(row, "trusted_whale_side_avg_holding_hours"), 2),
        "trusted_realized_roi_pct": _pct(_safe_float(row, "trusted_whale_side_realized_roi")),
        "pair_key": "|".join(_pair_group_key(row)),
    }


def _select_cases(records: list[dict[str, Any]], cases_per_category: int) -> list[dict[str, Any]]:
    """Select multiple deterministic cases covering improvements, regressions, and movement."""
    selected: list[dict[str, Any]] = []
    seen_exact: set[tuple[str, str, str, str]] = set()
    seen_market_side: set[tuple[str, str]] = set()
    ranking_groups = [
        sorted(records, key=lambda record: float(record["whale_error_improvement"]), reverse=True),
        sorted(records, key=lambda record: float(record["whale_error_improvement"])),
        sorted(records, key=lambda record: float(record["actual_movement"]), reverse=True),
        sorted(
            [record for record in records if bool(record["overlay_applies"])],
            key=lambda record: float(record["actual_movement"]),
            reverse=True,
        ),
    ]

    def add_from_rankings(*, require_new_market_side: bool) -> None:
        for ranking in ranking_groups:
            for record in ranking:
                row = record["row"]
                exact_key = (
                    str(row["market_slug"]),
                    str(row["side_label"]),
                    str(row["observation_time"]),
                    str(record["window"]),
                )
                market_side_key = (str(row["market_slug"]), str(row["side_label"]))
                if exact_key in seen_exact:
                    continue
                if require_new_market_side and market_side_key in seen_market_side:
                    continue
                seen_exact.add(exact_key)
                seen_market_side.add(market_side_key)
                selected.append(_case_payload(record))
                if len(selected) >= cases_per_category:
                    return

    add_from_rankings(require_new_market_side=True)
    if len(selected) < cases_per_category:
        add_from_rankings(require_new_market_side=False)
    return selected[:cases_per_category]


def _diagnostics_for_category(
    *,
    category: str,
    rows: list[dict[str, Any]],
    records: list[dict[str, Any]],
    min_category_rows: int,
) -> list[str]:
    """Return actionable diagnostics for one category."""
    diagnostics: list[str] = []
    health = _feature_health(rows)
    if int(health["row_count"]) < min_category_rows:
        diagnostics.append("Low row count; category-level model claims are not reliable yet.")
    if float(health["trusted_whale_activity_rows_pct"]) < 5.0:
        diagnostics.append("Trusted-whale coverage is sparse; broader whale-score features may matter more than trusted-only features.")
    if float(health["whale_holding_rows_pct"]) < 5.0:
        diagnostics.append("Candidate-whale holding/profit features are sparse; position reconstruction coverage should be improved.")
    if float(health["trusted_holding_rows_pct"]) < 5.0:
        diagnostics.append("Holding-time/profit strategy features are sparse; position reconstruction coverage should be improved.")
    for window_name in PREDICTION_WINDOWS:
        window_records = [record for record in records if record["window"] == window_name]
        if len(window_records) < min_category_rows:
            diagnostics.append(f"{window_name} has limited out-of-sample cases.")
            continue
        summary = _prediction_summary(window_records)
        if float(summary["whale_rmse_delta_vs_price_pts"]) > 0:
            diagnostics.append(f"{window_name} whale adjustment worsens RMSE versus price-only.")
        if int(summary["overlay_candidate_count"]) == 0 and "crypto" in category:
            diagnostics.append(f"{window_name} has no trend-overlay candidates despite crypto category labeling.")
    return sorted(set(diagnostics))


def _category_report_rows(
    *,
    rows: list[dict[str, Any]],
    records: list[dict[str, Any]],
    cases_per_category: int,
    min_category_rows: int,
) -> list[dict[str, Any]]:
    """Build report sections grouped by event category."""
    rows_by_category: dict[str, list[dict[str, Any]]] = {}
    records_by_category: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_category.setdefault(_category_label(row), []).append(row)
    for record in records:
        records_by_category.setdefault(str(record["category"]), []).append(record)

    category_rows: list[dict[str, Any]] = []
    for category in sorted(rows_by_category, key=lambda value: (-len(rows_by_category[value]), value)):
        category_records = records_by_category.get(category, [])
        windows = {
            window_name: _prediction_summary(
                [record for record in category_records if record["window"] == window_name]
            )
            for window_name in PREDICTION_WINDOWS
        }
        pair_consistency = {
            window_name: _category_pair_summary(
                [record for record in category_records if record["window"] == window_name]
            )
            for window_name in PREDICTION_WINDOWS
        }
        category_rows.append(
            {
                "category": category,
                "feature_health": _feature_health(rows_by_category[category]),
                "windows": windows,
                "pair_side_consistency": pair_consistency,
                "cases": _select_cases(category_records, cases_per_category),
                "diagnostics": _diagnostics_for_category(
                    category=category,
                    rows=rows_by_category[category],
                    records=category_records,
                    min_category_rows=min_category_rows,
                ),
            }
        )
    return category_rows


def _overall_report(
    *,
    rows: list[dict[str, Any]],
    records: list[dict[str, Any]],
    category_rows: list[dict[str, Any]],
    prediction_error: str | None,
) -> dict[str, Any]:
    """Return top-level test summary and issue counts."""
    issue_counter = Counter(
        diagnostic
        for category in category_rows
        for diagnostic in category.get("diagnostics", [])
    )
    return {
        "row_count": len(rows),
        "condition_count": _unique_condition_count(rows),
        "category_count": len(category_rows),
        "prediction_record_count": len(records),
        "prediction_error": prediction_error,
        "windows": {
            window_name: _prediction_summary([record for record in records if record["window"] == window_name])
            for window_name in PREDICTION_WINDOWS
        },
        "feature_health": _feature_health(rows),
        "top_issue_counts": [
            {"issue": issue, "category_count": count}
            for issue, count in issue_counter.most_common(10)
        ],
    }


def _markdown_value(value: Any) -> str:
    """Return compact markdown display value."""
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _write_markdown(payload: dict[str, Any], markdown_path: Path) -> None:
    """Write a human-readable category validation report."""
    lines = [
        "# ML Category Validation Report",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Dataset: `{payload['dataset_path']}`",
        f"Comparison: `{payload['comparison_path']}`",
        f"Rows: `{payload['overall']['row_count']}`",
        f"Conditions: `{payload['overall']['condition_count']}`",
        f"Categories: `{payload['overall']['category_count']}`",
    ]
    if payload["overall"].get("prediction_error"):
        lines.append(f"Prediction validation status: `{payload['overall']['prediction_error']}`")
    lines.extend(
        [
            "",
            "## Overall Window Metrics",
            "",
            "| Window | Rows | Price RMSE | Whale RMSE | RMSE Delta | Improving Rows | Overlay Candidates |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, summary in payload["overall"]["windows"].items():
        lines.append(
            "| {window} | {rows} | {price} | {whale} | {delta} | {improving}% | {overlay} |".format(
                window=window_name,
                rows=summary["row_count"],
                price=_markdown_value(summary["price_rmse_pts"]),
                whale=_markdown_value(summary["whale_adjusted_rmse_pts"]),
                delta=_markdown_value(summary["whale_rmse_delta_vs_price_pts"]),
                improving=_markdown_value(summary["improving_row_pct"]),
                overlay=summary["overlay_candidate_count"],
            )
        )

    lines.extend(
        [
            "",
            "## Category Summary",
            "",
            "| Category | Rows | Conditions | Whale Rows | Whale Holding Rows | Trusted Whale Rows | Trusted Holding Rows | 12h RMSE Delta | 24h RMSE Delta | Cases | Main Diagnostics |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for category in payload["categories"]:
        health = category["feature_health"]
        lines.append(
            "| {category} | {rows} | {conditions} | {whale}% | {whale_holding}% | {trusted}% | {holding}% | {delta12} | {delta24} | {cases} | {diagnostics} |".format(
                category=category["category"],
                rows=health["row_count"],
                conditions=health["condition_count"],
                whale=_markdown_value(health["whale_activity_rows_pct"]),
                whale_holding=_markdown_value(health["whale_holding_rows_pct"]),
                trusted=_markdown_value(health["trusted_whale_activity_rows_pct"]),
                holding=_markdown_value(health["trusted_holding_rows_pct"]),
                delta12=_markdown_value(category["windows"]["12h"]["whale_rmse_delta_vs_price_pts"]),
                delta24=_markdown_value(category["windows"]["24h"]["whale_rmse_delta_vs_price_pts"]),
                cases=len(category["cases"]),
                diagnostics="; ".join(category["diagnostics"][:3]) or "None",
            )
        )

    lines.extend(["", "## Selected Test Cases", ""])
    for category in payload["categories"]:
        lines.append(f"### {category['category']}")
        if not category["cases"]:
            lines.append("")
            lines.append("No out-of-sample prediction cases were available.")
            lines.append("")
            continue
        lines.extend(
            [
                "",
                "| Reason | Window | Market | Side | Actual | Price-only | Whale-adjusted | Improvement | Whale Users | Trusted Users |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for case in category["cases"]:
            lines.append(
                "| {reason} | {window} | {market} | {side} | {actual} | {price} | {whale} | {improvement} | {whales} | {trusted} |".format(
                    reason=case["reason"],
                    window=case["window"],
                    market=case["market_slug"],
                    side=case["side_label"],
                    actual=_markdown_value(case["actual_future_odds_pct"]),
                    price=_markdown_value(case["price_only_predicted_odds_pct"]),
                    whale=_markdown_value(case["whale_adjusted_predicted_odds_pct"]),
                    improvement=_markdown_value(case["whale_error_improvement_pts"]),
                    whales=case["whale_distinct_users"],
                    trusted=case["trusted_whale_distinct_users"],
                )
            )
        lines.append("")

    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def evaluate_category_validation(
    *,
    dataset_path: Path,
    comparison_path: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    cases_per_category: int,
    min_category_rows: int,
) -> dict[str, Any]:
    """Evaluate current residual movement predictions across all market categories."""
    selected_specs = _selected_prediction_specs(comparison_path)
    rows = _filter_rows_by_regime(_load_training_rows(dataset_path), REGIME_TRADE_COVERED)
    _enrich_trend_features(rows)
    prediction_error: str | None = None
    try:
        predictions = _predict_rolling_rows(rows=rows, selected_specs=selected_specs)
        records = _prediction_records(predictions=predictions)
    except RuntimeError as exc:
        prediction_error = str(exc)
        records = []
    categories = _category_report_rows(
        rows=rows,
        records=records,
        cases_per_category=cases_per_category,
        min_category_rows=min_category_rows,
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "comparison_path": str(comparison_path),
        "estimator": "mixed_by_window",
        "selected_configs": {window: spec["selected_config"] for window, spec in selected_specs.items()},
        "selected_model_specs": selected_specs,
        "regime": REGIME_TRADE_COVERED,
        "cases_per_category": cases_per_category,
        "min_category_rows": min_category_rows,
        "overall": _overall_report(
            rows=rows,
            records=records,
            category_rows=categories,
            prediction_error=prediction_error,
        ),
        "categories": categories,
    }
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(payload, output_markdown_path)
    return payload


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Evaluate residual ML predictions by market category.")
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--comparison-path", default=str(DEFAULT_COMPARISON_PATH))
    parser.add_argument("--output-json-path", default=str(DEFAULT_OUTPUT_JSON_PATH))
    parser.add_argument("--output-markdown-path", default=str(DEFAULT_OUTPUT_MARKDOWN_PATH))
    parser.add_argument("--cases-per-category", type=int, default=5)
    parser.add_argument("--min-category-rows", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    payload = evaluate_category_validation(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        output_json_path=Path(args.output_json_path),
        output_markdown_path=Path(args.output_markdown_path),
        cases_per_category=args.cases_per_category,
        min_category_rows=args.min_category_rows,
    )
    print(
        json.dumps(
            {
                "output_json_path": args.output_json_path,
                "output_markdown_path": args.output_markdown_path,
                "row_count": payload["overall"]["row_count"],
                "condition_count": payload["overall"]["condition_count"],
                "category_count": payload["overall"]["category_count"],
                "top_issue_counts": payload["overall"]["top_issue_counts"][:5],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
