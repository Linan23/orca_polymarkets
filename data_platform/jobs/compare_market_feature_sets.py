"""Compare price-only and price-plus-whale market models on the same grouped split."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.ml.market_baseline_model import DEFAULT_COMPARISON_PATH, compare_price_vs_whale_market_models
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Compare market feature sets on the grouped time split.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--comparison-path",
        default=str(DEFAULT_COMPARISON_PATH),
        help="JSON output path for the comparison summary.",
    )
    parser.add_argument("--train-fraction", type=float, default=0.75, help="Oldest-condition fraction used for training.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    result = compare_price_vs_whale_market_models(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        train_fraction=args.train_fraction,
        random_state=args.random_state,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
