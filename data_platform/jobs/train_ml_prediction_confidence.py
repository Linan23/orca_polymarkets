"""Train market-profile prediction confidence from closed/validated outcomes."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ml.prediction_confidence import (
    DEFAULT_CONFIDENCE_MODEL_PATH,
    MIN_TRAIN_ROWS,
    train_prediction_confidence_model,
    write_confidence_artifact,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--platform-name", default=os.getenv("ML_PREDICTION_CONFIDENCE_PLATFORM", "polymarket"))
    parser.add_argument(
        "--output-path",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_MODEL_PATH", str(DEFAULT_CONFIDENCE_MODEL_PATH)),
        help="JSON artifact path used by market-profile snapshot generation.",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=int(os.getenv("ML_PREDICTION_CONFIDENCE_MIN_TRAIN_ROWS", str(MIN_TRAIN_ROWS))),
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=float(os.getenv("ML_PREDICTION_CONFIDENCE_TEST_FRACTION", "0.25")),
        help="Newest chronological share held out for validation.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        artifact = train_prediction_confidence_model(
            session,
            platform_name=str(args.platform_name),
            min_train_rows=max(int(args.min_train_rows), 1),
            test_fraction=float(args.test_fraction),
        )
    output_path = Path(args.output_path)
    write_confidence_artifact(artifact, output_path)
    summary = {
        "ok": True,
        "output_path": str(output_path),
        "model_version": artifact.get("model_version"),
        "trained_at": artifact.get("trained_at"),
        "row_count": artifact.get("row_count"),
        "windows": artifact.get("windows"),
    }
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
