"""Analyze residual whale signal beyond price on the market dataset."""

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
    DEFAULT_WHALE_SIGNAL_ANALYSIS_PATH,
    analyze_market_whale_signal,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Analyze residual whale signal beyond price.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--analysis-path",
        default=str(DEFAULT_WHALE_SIGNAL_ANALYSIS_PATH),
        help="JSON output path for the whale-signal analysis report.",
    )
    parser.add_argument(
        "--estimator",
        choices=("lightgbm", "random_forest"),
        default="lightgbm",
        help="Estimator family to use for residual modeling.",
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
    result = analyze_market_whale_signal(
        dataset_path=Path(args.dataset_path),
        analysis_path=Path(args.analysis_path),
        estimator_type=args.estimator,
        train_fraction=args.train_fraction,
        random_state=args.random_state,
        min_horizon_hours=args.min_horizon_hours,
        max_horizon_hours=args.max_horizon_hours,
        regime=args.regime,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
