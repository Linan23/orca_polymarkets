"""Compare residual whale movement model families side by side."""

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
    DEFAULT_MOVEMENT_RESIDUAL_MODEL_COMPARISON_PATH,
    DEFAULT_WEEK10_11_RESIDUAL_MODEL_COMPARISON_PATH,
    compare_market_movement_residual_model_families,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def _split_csv(value: str) -> tuple[str, ...]:
    """Return non-empty comma-separated string values."""
    return tuple(item.strip() for item in str(value or "").split(",") if item.strip())


def _split_float_csv(value: str) -> tuple[float, ...]:
    """Return non-empty comma-separated float values."""
    return tuple(float(item.strip()) for item in str(value or "").split(",") if item.strip())


def _split_int_csv(value: str) -> tuple[int, ...]:
    """Return non-empty comma-separated integer values."""
    return tuple(int(item.strip()) for item in str(value or "").split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Compare residual whale movement model families.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--comparison-path",
        default=str(DEFAULT_MOVEMENT_RESIDUAL_MODEL_COMPARISON_PATH),
        help="JSON output path for the residual model-family comparison.",
    )
    parser.add_argument(
        "--markdown-path",
        default=str(DEFAULT_WEEK10_11_RESIDUAL_MODEL_COMPARISON_PATH),
        help="Markdown output path for the Week 10-11 comparison report.",
    )
    parser.add_argument(
        "--estimators",
        default="random_forest,ridge,lightgbm",
        help=(
            "Comma-separated residual estimator/profile names to compare. "
            "Supported values include random_forest, ridge, lightgbm, lightgbm_conservative."
        ),
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
    parser.add_argument(
        "--segment",
        default="",
        help="Optional comma-separated research-focus segment filter, for example short_non_crypto.",
    )
    parser.add_argument(
        "--exclude-family",
        default="",
        help="Optional comma-separated market family exclusion, for example crypto_updown.",
    )
    return parser.parse_args()


def _compact_result(result: dict) -> dict:
    """Return a compact CLI summary while full detail stays in the JSON report."""
    summary = result["summary"]
    recommendation = summary.get("recommendation", {})
    models = {
        estimator_type: {
            "score": model.get("score"),
            "overall_residual_whale_lift_demonstrated": model.get("overall_residual_whale_lift_demonstrated"),
            "windows": {
                window_name: {
                    "selected_config": window.get("selected_config"),
                    "rmse_delta": window.get("rmse_delta"),
                    "stable_whale_feature_count": window.get("stable_whale_feature_count"),
                    "passing_fold_count": window.get("passing_fold_count"),
                    "worsening_research_segment_count": window.get("worsening_research_segment_count"),
                    "whale_lift_demonstrated": window.get("whale_lift_demonstrated"),
                }
                for window_name, window in model.get("windows", {}).items()
            },
        }
        for estimator_type, model in summary.get("models", {}).items()
    }
    return {
        "comparison_path": result["comparison_path"],
        "markdown_path": result["markdown_path"],
        "detail_report_dir": result["detail_report_dir"],
        "row_count": summary.get("row_count"),
        "regime": summary.get("regime"),
        "research_segments": summary.get("research_segments"),
        "exclude_market_families": summary.get("exclude_market_families"),
        "default_estimator": recommendation.get("default_estimator"),
        "all_required_windows_lift": recommendation.get("all_required_windows_lift"),
        "window_recommendations": recommendation.get("window_recommendations"),
        "models": models,
    }


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    result = compare_market_movement_residual_model_families(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        markdown_path=Path(args.markdown_path),
        estimator_types=_split_csv(args.estimators),
        train_fraction=args.train_fraction,
        random_state=args.random_state,
        min_horizon_hours=args.min_horizon_hours,
        max_horizon_hours=args.max_horizon_hours,
        regime=args.regime,
        research_segments=_split_csv(args.segment),
        exclude_market_families=_split_csv(args.exclude_family),
        selector_thresholds=_split_float_csv(args.selector_thresholds),
        selector_max_features=_split_int_csv(args.selector_max_features),
    )
    print(json.dumps(_compact_result(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
