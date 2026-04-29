"""Run the Week 10-11 movement-model tuning report."""

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
    DEFAULT_MOVEMENT_TUNING_REPORT_PATH,
    DEFAULT_WEEK10_11_MOVEMENT_REPORT_PATH,
    tune_market_movement_models,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def _split_csv(value: str) -> tuple[str, ...]:
    """Return non-empty comma-separated CLI values."""
    return tuple(item.strip() for item in str(value or "").split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Tune 12h/24h whale movement models and write a Week 10-11 report.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--report-path",
        default=str(DEFAULT_MOVEMENT_TUNING_REPORT_PATH),
        help="JSON output path for compact tuning results.",
    )
    parser.add_argument(
        "--markdown-path",
        default=str(DEFAULT_WEEK10_11_MOVEMENT_REPORT_PATH),
        help="Markdown output path for the Week 10-11 summary artifact.",
    )
    parser.add_argument(
        "--profiles",
        default="rf_shallow,rf_regularized,rf_current",
        help="Comma-separated profile names. Default skips LightGBM for faster iteration.",
    )
    parser.add_argument(
        "--estimators",
        default="",
        help="Optional comma-separated estimator filter: random_forest,lightgbm.",
    )
    parser.add_argument(
        "--regime",
        choices=("all", "trade_covered", "cold_start"),
        default="all",
        help="Optional regime filter. Use trade_covered to judge whale lift only where pre-cutoff trades exist.",
    )
    parser.add_argument("--train-fraction", type=float, default=0.75, help="Oldest-condition fraction used for training.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--min-horizon-hours", type=float, default=None, help="Optional inclusive minimum horizon filter.")
    parser.add_argument("--max-horizon-hours", type=float, default=None, help="Optional inclusive maximum horizon filter.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    result = tune_market_movement_models(
        dataset_path=Path(args.dataset_path),
        report_path=Path(args.report_path),
        markdown_path=Path(args.markdown_path),
        train_fraction=args.train_fraction,
        random_state=args.random_state,
        min_horizon_hours=args.min_horizon_hours,
        max_horizon_hours=args.max_horizon_hours,
        regime=args.regime,
        profile_names=_split_csv(args.profiles),
        estimator_types=_split_csv(args.estimators),
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
