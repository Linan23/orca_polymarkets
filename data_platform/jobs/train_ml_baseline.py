"""Train the first baseline model on the exported ML dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.ml.baseline_model import (
    DEFAULT_DATASET_PATH,
    DEFAULT_IMPORTANCE_PATH,
    DEFAULT_METRICS_PATH,
    DEFAULT_MODEL_PATH,
    train_profitability_baseline,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Train the baseline profitability model.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_ml_dataset.py.",
    )
    parser.add_argument(
        "--model-path",
        default=str(DEFAULT_MODEL_PATH),
        help="Pickle output path for the fitted baseline model.",
    )
    parser.add_argument(
        "--metrics-path",
        default=str(DEFAULT_METRICS_PATH),
        help="JSON output path for model metrics.",
    )
    parser.add_argument(
        "--feature-importance-path",
        default=str(DEFAULT_IMPORTANCE_PATH),
        help="CSV output path for feature importances.",
    )
    parser.add_argument("--test-size", type=float, default=0.25, help="Fraction of rows reserved for evaluation.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducible splits.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    summary = train_profitability_baseline(
        dataset_path=Path(args.dataset_path),
        model_path=Path(args.model_path),
        metrics_path=Path(args.metrics_path),
        feature_importance_path=Path(args.feature_importance_path),
        test_size=args.test_size,
        random_state=args.random_state,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
