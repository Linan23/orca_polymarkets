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
    STRONG_PRECISION_TARGET,
    WATCH_PRECISION_TARGET,
    confidence_promotion_decision,
    load_confidence_artifact,
    load_validated_confidence_rows,
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
        "--active-model-path",
        "--confidence-model-path",
        dest="active_model_path",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_MODEL_PATH", str(DEFAULT_CONFIDENCE_MODEL_PATH)),
        help="Active JSON confidence artifact used by market-profile prediction snapshots.",
    )
    parser.add_argument(
        "--candidate-output-dir",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_CANDIDATE_DIR", "data_platform/runtime/ml/candidates"),
    )
    parser.add_argument(
        "--promotion-manifest-path",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_PROMOTION_MANIFEST", "data_platform/runtime/ml/model_promotion_manifest.jsonl"),
    )
    parser.add_argument(
        "--promotion-mode",
        choices=("gated", "always", "never"),
        default=os.getenv("ML_PREDICTION_CONFIDENCE_PROMOTION_MODE", "gated"),
        help="Use gated promotion, always promote candidates, or never replace the active artifact.",
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
    parser.add_argument(
        "--watch-precision-target",
        type=float,
        default=float(os.getenv("ML_PREDICTION_WATCH_PRECISION_TARGET", str(WATCH_PRECISION_TARGET))),
    )
    parser.add_argument(
        "--strong-precision-target",
        type=float,
        default=float(os.getenv("ML_PREDICTION_STRONG_PRECISION_TARGET", str(STRONG_PRECISION_TARGET))),
    )
    parser.add_argument(
        "--max-mae-regression-pts",
        type=float,
        default=float(os.getenv("ML_PREDICTION_MAX_MAE_REGRESSION_PTS", "0.5")),
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


def _append_jsonl(path: Path, record: dict[str, object]) -> None:
    """Append a compact JSON record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":"), default=str))
        handle.write("\n")


def _candidate_artifact_path(candidate_dir: Path, *, as_of: datetime, platform_name: str) -> Path:
    """Return a stable candidate artifact path for one training cycle."""
    stamp = as_of.strftime("%Y%m%dT%H%M%SZ")
    safe_platform = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in platform_name)
    return candidate_dir / f"{stamp}_{safe_platform}_market_prediction_confidence_candidate.json"


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    as_of = datetime.now(timezone.utc)
    active_model_path = Path(args.active_model_path)
    candidate_output_dir = Path(args.candidate_output_dir)
    promotion_manifest_path = Path(args.promotion_manifest_path)
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
            watch_precision_target=float(args.watch_precision_target),
            strong_precision_target=float(args.strong_precision_target),
        )
        candidate_path = _candidate_artifact_path(candidate_output_dir, as_of=as_of, platform_name=str(args.platform_name))
        active_artifact = load_confidence_artifact(active_model_path)
        validation_rows = load_validated_confidence_rows(session, platform_name=str(args.platform_name))
        promotion = confidence_promotion_decision(
            candidate_artifact=confidence_artifact,
            active_artifact=active_artifact,
            validation_rows=validation_rows,
            min_train_rows=max(int(args.min_train_rows), 1),
            test_fraction=float(args.test_fraction),
            watch_precision_target=float(args.watch_precision_target),
            max_mae_regression_pts=float(args.max_mae_regression_pts),
        )
        if args.promotion_mode == "always":
            promotion["promotion_status"] = "promoted"
            promotion["promotion_reason"] = "promotion-mode always"
        elif args.promotion_mode == "never":
            promotion["promotion_status"] = "rejected"
            promotion["promotion_reason"] = "promotion-mode never"

        confidence_artifact.update(
            {
                "promotion_status": promotion.get("promotion_status"),
                "promotion_reason": promotion.get("promotion_reason"),
                "previous_model_trained_at": promotion.get("previous_model_trained_at"),
                "candidate_metrics": promotion.get("candidate_metrics"),
                "active_metrics": promotion.get("active_metrics"),
                "window_metrics": (promotion.get("candidate_metrics") or {}).get("windows"),
            }
        )
        write_confidence_artifact(confidence_artifact, candidate_path)
        if promotion.get("promotion_status") == "promoted":
            write_confidence_artifact(confidence_artifact, active_model_path)
        _append_jsonl(
            promotion_manifest_path,
            {
                "as_of": as_of.isoformat(),
                "platform": args.platform_name,
                "promotion_mode": args.promotion_mode,
                "candidate_path": str(candidate_path),
                "active_model_path": str(active_model_path),
                "promotion_status": promotion.get("promotion_status"),
                "promotion_reason": promotion.get("promotion_reason"),
                "candidate_metrics": promotion.get("candidate_metrics"),
                "active_metrics": promotion.get("active_metrics"),
            },
        )
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
            confidence_model_path=active_model_path,
        )

    summary = {
        "ok": bool(validation_summary.get("ok")) and bool(snapshot_summary.get("ok")),
        "as_of": as_of.isoformat(),
        "platform": args.platform_name,
        "validation": validation_summary,
        "confidence_training": {
            "ok": True,
            "active_model_path": str(active_model_path),
            "candidate_path": str(candidate_path),
            "promotion_manifest_path": str(promotion_manifest_path),
            "promotion_status": promotion.get("promotion_status"),
            "promotion_reason": promotion.get("promotion_reason"),
            "model_version": confidence_artifact.get("model_version"),
            "trained_at": confidence_artifact.get("trained_at"),
            "row_count": confidence_artifact.get("row_count"),
            "windows": confidence_artifact.get("windows"),
            "candidate_metrics": promotion.get("candidate_metrics"),
            "active_metrics": promotion.get("active_metrics"),
        },
        "snapshot_generation": snapshot_summary,
    }
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
