"""Analyze residual whale movement signal after price-only prediction."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.ml.market_baseline_model import (
    DEFAULT_MOVEMENT_RESIDUAL_REPORT_PATH,
    DEFAULT_WEEK10_11_RESIDUAL_REPORT_PATH,
    analyze_market_movement_residuals,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def _split_float_csv(value: str) -> tuple[float, ...]:
    """Return non-empty comma-separated float values."""
    return tuple(float(item.strip()) for item in str(value or "").split(",") if item.strip())


def _split_int_csv(value: str) -> tuple[int, ...]:
    """Return non-empty comma-separated integer values."""
    return tuple(int(item.strip()) for item in str(value or "").split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Analyze residual whale signal for 12h/24h movement.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--report-path",
        default=str(DEFAULT_MOVEMENT_RESIDUAL_REPORT_PATH),
        help="JSON output path for residual movement analysis.",
    )
    parser.add_argument(
        "--markdown-path",
        default=str(DEFAULT_WEEK10_11_RESIDUAL_REPORT_PATH),
        help="Markdown output path for the Week 10-11 residual table.",
    )
    parser.add_argument(
        "--estimator",
        choices=("random_forest", "lightgbm"),
        default="random_forest",
        help="Estimator family used for price-only and residual models.",
    )
    parser.add_argument(
        "--regime",
        choices=("all", "trade_covered", "cold_start"),
        default="trade_covered",
        help="Optional regime filter. trade_covered is the default for whale residual claims.",
    )
    parser.add_argument(
        "--selector-thresholds",
        default="0.01,0.02,0.05",
        help="Comma-separated absolute-correlation thresholds for whale feature selection.",
    )
    parser.add_argument(
        "--selector-max-features",
        default="8,16,24",
        help="Comma-separated selected whale feature caps.",
    )
    parser.add_argument("--train-fraction", type=float, default=0.75, help="Oldest-condition fraction used for training.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--min-horizon-hours", type=float, default=None, help="Optional inclusive minimum horizon filter.")
    parser.add_argument("--max-horizon-hours", type=float, default=None, help="Optional inclusive maximum horizon filter.")
    return parser.parse_args()


def _compact_result(result: dict) -> dict:
    """Return a compact CLI summary while full detail stays in the JSON report."""
    summary = result["summary"]
    windows = {}
    for window_name, window_summary in summary.get("windows", {}).items():
        recommendation = window_summary.get("recommendation", {})
        windows[window_name] = {
            "selected_config": recommendation.get("selected_config"),
            "selected_rolling_rmse_delta": recommendation.get("selected_rolling_rmse_delta"),
            "selected_stable_whale_feature_count": recommendation.get("selected_stable_whale_feature_count"),
            "raw_best_config": recommendation.get("best_config"),
            "raw_best_rolling_rmse_delta": recommendation.get("best_rolling_rmse_delta"),
            "raw_best_stable_whale_feature_count": recommendation.get("raw_best_stable_whale_feature_count"),
            "whale_lift_demonstrated": recommendation.get("whale_lift_demonstrated"),
        }
    return {
        "report_path": result["report_path"],
        "markdown_path": result["markdown_path"],
        "row_count": summary.get("row_count"),
        "regime": summary.get("regime"),
        "overall_residual_whale_lift_demonstrated": summary.get("overall_residual_whale_lift_demonstrated"),
        "windows": windows,
    }


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    result = analyze_market_movement_residuals(
        dataset_path=Path(args.dataset_path),
        report_path=Path(args.report_path),
        markdown_path=Path(args.markdown_path),
        estimator_type=args.estimator,
        train_fraction=args.train_fraction,
        random_state=args.random_state,
        min_horizon_hours=args.min_horizon_hours,
        max_horizon_hours=args.max_horizon_hours,
        regime=args.regime,
        selector_thresholds=_split_float_csv(args.selector_thresholds),
        selector_max_features=_split_int_csv(args.selector_max_features),
    )
    print(json.dumps(_compact_result(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
