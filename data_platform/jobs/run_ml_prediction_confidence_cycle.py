"""Validate matured predictions, retrain confidence, then generate fresh snapshots."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.jobs.generate_ml_market_prediction_snapshots import (
    DEFAULT_FEATURE_SCHEMA_VERSION,
    DEFAULT_LIVE_FEATURE_MARKET_LIMIT,
    DEFAULT_MODEL_VERSION,
    generate_prediction_snapshots,
)
from data_platform.jobs.validate_ml_market_predictions import (
    DEFAULT_LIMIT as DEFAULT_VALIDATION_LIMIT,
    DEFAULT_TARGET_TOLERANCE_MINUTES,
    validate_predictions,
)
from data_platform.ml.prediction_confidence import (
    DEFAULT_CONFIDENCE_MODEL_PATH,
    MIN_TRAIN_ROWS,
    train_prediction_confidence_model,
    write_confidence_artifact,
)
from data_platform.services.ml_reports import WHALE_ANCHORED_DELTA_JSON_PATH


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--platform-name", default=os.getenv("ML_PREDICTION_CYCLE_PLATFORM", "polymarket"))
    parser.add_argument("--create-tables", action="store_true", help="Create ML tables when running locally.")
    parser.add_argument(
        "--validation-limit",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_LIMIT", str(DEFAULT_VALIDATION_LIMIT))),
        help="Maximum matured prediction snapshots to validate before training. Use 0 for no cap.",
    )
    parser.add_argument(
        "--target-tolerance-minutes",
        type=int,
        default=int(os.getenv("ML_PREDICTION_VALIDATION_TARGET_TOLERANCE_MINUTES", str(DEFAULT_TARGET_TOLERANCE_MINUTES))),
    )
    parser.add_argument("--revalidate", action="store_true", help="Rescore already validated snapshots before training.")
    parser.add_argument(
        "--confidence-model-path",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_MODEL_PATH", str(DEFAULT_CONFIDENCE_MODEL_PATH)),
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
    )
    parser.add_argument("--include-closed", action="store_true", help="Also snapshot closed markets for local checks.")
    parser.add_argument("--snapshot-limit", type=int, default=0, help="Maximum active markets to snapshot. Use 0 for no cap.")
    parser.add_argument("--local-report-path", default=str(WHALE_ANCHORED_DELTA_JSON_PATH))
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    parser.add_argument("--feature-schema-version", default=DEFAULT_FEATURE_SCHEMA_VERSION)
    parser.add_argument(
        "--live-feature-market-limit",
        type=int,
        default=int(os.getenv("ML_LIVE_FEATURE_MARKET_LIMIT", str(DEFAULT_LIVE_FEATURE_MARKET_LIMIT))),
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    as_of = datetime.now(timezone.utc)
    confidence_model_path = Path(args.confidence_model_path)
    with session_scope(args.database_url or None) as session:
        validation_summary = validate_predictions(
            session,
            platform_name=str(args.platform_name),
            as_of=as_of,
            limit=int(args.validation_limit),
            target_tolerance_minutes=max(int(args.target_tolerance_minutes), 0),
            create_table=bool(args.create_tables),
            revalidate=bool(args.revalidate),
        )
        confidence_artifact = train_prediction_confidence_model(
            session,
            platform_name=str(args.platform_name),
            min_train_rows=max(int(args.min_train_rows), 1),
            test_fraction=float(args.test_fraction),
        )
        write_confidence_artifact(confidence_artifact, confidence_model_path)
        snapshot_summary = generate_prediction_snapshots(
            session,
            platform_name=str(args.platform_name),
            include_closed=bool(args.include_closed),
            limit=int(args.snapshot_limit),
            local_report_path=Path(args.local_report_path),
            model_version=str(args.model_version),
            feature_schema_version=str(args.feature_schema_version),
            create_table=bool(args.create_tables),
            live_feature_market_limit=int(args.live_feature_market_limit),
            confidence_model_path=confidence_model_path,
        )

    summary = {
        "ok": bool(validation_summary.get("ok")) and bool(snapshot_summary.get("ok")),
        "as_of": as_of.isoformat(),
        "platform": args.platform_name,
        "validation": validation_summary,
        "confidence_training": {
            "ok": True,
            "output_path": str(confidence_model_path),
            "model_version": confidence_artifact.get("model_version"),
            "trained_at": confidence_artifact.get("trained_at"),
            "row_count": confidence_artifact.get("row_count"),
            "windows": confidence_artifact.get("windows"),
        },
        "snapshot_generation": snapshot_summary,
    }
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
