"""Train the canonical market ML model with optional rolling diagnostics."""

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
    DEFAULT_LIGHTGBM_IMPORTANCE_PATH,
    DEFAULT_LIGHTGBM_METRICS_PATH,
    DEFAULT_LIGHTGBM_MODEL_PATH,
    DEFAULT_TRAINING_REPORT_PATH,
    TASK_MARKET_MOVEMENT_12H,
    TASK_MARKET_MOVEMENT_24H,
    TASK_MARKET_OUTCOME,
    TASK_WHALE_SIGNAL,
    train_market_model,
)
from data_platform.ml.market_dataset_builder import DEFAULT_DATASET_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Train the canonical market ML model.")
    parser.add_argument(
        "--dataset-path",
        default=str(DEFAULT_DATASET_PATH),
        help="CSV input path produced by export_market_ml_dataset.py.",
    )
    parser.add_argument(
        "--task",
        choices=("outcome", "whale_signal", "movement_12h", "movement_24h"),
        default="outcome",
        help="Model task to train.",
    )
    parser.add_argument(
        "--estimator",
        choices=("lightgbm", "random_forest"),
        default="lightgbm",
        help="Estimator family to use.",
    )
    parser.add_argument(
        "--feature-set",
        choices=("full", "price_only", "whale_only", "price_plus_whale", "cold_start"),
        default="",
        help="Optional feature-set override. Leave empty to use the task default.",
    )
    parser.add_argument(
        "--evaluation-mode",
        choices=("single_split", "rolling"),
        default="rolling",
        help="Whether to include grouped rolling diagnostics in the report.",
    )
    parser.add_argument(
        "--regime",
        choices=("all", "trade_covered", "cold_start"),
        default="all",
        help="Optional regime filter. Use trade_covered or cold_start to train that slice separately.",
    )
    parser.add_argument(
        "--model-path",
        default=str(DEFAULT_LIGHTGBM_MODEL_PATH),
        help="Pickle output path for the fitted single-split model.",
    )
    parser.add_argument(
        "--metrics-path",
        default=str(DEFAULT_LIGHTGBM_METRICS_PATH),
        help="JSON output path for single-split metrics.",
    )
    parser.add_argument(
        "--feature-importance-path",
        default=str(DEFAULT_LIGHTGBM_IMPORTANCE_PATH),
        help="CSV output path for feature importances.",
    )
    parser.add_argument(
        "--report-path",
        default=str(DEFAULT_TRAINING_REPORT_PATH),
        help="JSON output path for the full training/evaluation report.",
    )
    parser.add_argument("--train-fraction", type=float, default=0.75, help="Oldest-condition fraction used for training.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--min-horizon-hours", type=float, default=None, help="Optional inclusive minimum horizon filter.")
    parser.add_argument("--max-horizon-hours", type=float, default=None, help="Optional inclusive maximum horizon filter.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    task_lookup = {
        "outcome": TASK_MARKET_OUTCOME,
        "whale_signal": TASK_WHALE_SIGNAL,
        "movement_12h": TASK_MARKET_MOVEMENT_12H,
        "movement_24h": TASK_MARKET_MOVEMENT_24H,
    }
    task = task_lookup[args.task]
    summary = train_market_model(
        dataset_path=Path(args.dataset_path),
        model_path=Path(args.model_path),
        metrics_path=Path(args.metrics_path),
        feature_importance_path=Path(args.feature_importance_path),
        report_path=Path(args.report_path),
        task=task,
        estimator_type=args.estimator,
        feature_set=args.feature_set or None,
        evaluation_mode=args.evaluation_mode,
        train_fraction=args.train_fraction,
        random_state=args.random_state,
        min_horizon_hours=args.min_horizon_hours,
        max_horizon_hours=args.max_horizon_hours,
        regime=args.regime,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
