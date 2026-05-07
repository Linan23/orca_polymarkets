"""Validate gated ML confidence artifact promotion decisions."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.ml.prediction_confidence import FEATURE_NAMES, confidence_promotion_decision


def _artifact(*, coefficient: float, threshold: float = 0.7, trained_at: str = "2026-05-07T00:00:00+00:00") -> dict[str, Any]:
    coefficients = [0.0 for _ in FEATURE_NAMES]
    coefficients[FEATURE_NAMES.index("predicted_delta_pts")] = coefficient
    window = {
        "status": "trained",
        "row_count": 8,
        "train_rows": 4,
        "test_rows": 4,
        "feature_names": FEATURE_NAMES,
        "scaler_mean": [0.0 for _ in FEATURE_NAMES],
        "scaler_scale": [1.0 for _ in FEATURE_NAMES],
        "coefficients": coefficients,
        "intercept": 0.0,
        "thresholds": {
            "watch": {"threshold": threshold, "precision": 1.0, "coverage": 0.75},
            "strong": {"threshold": 0.9, "precision": 1.0, "coverage": 0.75},
        },
        "metrics": {},
        "error_bins": [],
    }
    return {
        "model_version": "test_confidence_model",
        "trained_at": trained_at,
        "windows": {"12": dict(window), "24": dict(window)},
    }


def _row(window_hours: int, *, delta: float, direction_match: bool, error: float) -> dict[str, Any]:
    return {
        "prediction_window_hours": window_hours,
        "prediction_payload": {
            "prediction_window_hours": window_hours,
            "current_odds_pct": 50.0,
            "predicted_future_odds_pct": 50.0 + delta,
            "predicted_delta_pts": delta,
            "predicted_direction": "up" if delta >= 0 else "down",
            "direction_signal_tier": "watch",
            "prediction_source": "live_whale_signal_model",
            "whale_anchor": {"side_total_pressure": 1000, "side_net_pressure": 500, "event_count": 3, "trusted_event_count": 2},
        },
        "signal_tier": "watch",
        "prediction_source": "live_whale_signal_model",
        "predicted_delta_pts": delta,
        "predicted_direction": "up" if delta >= 0 else "down",
        "direction_match": direction_match,
        "absolute_error_pts": error,
    }


def _validation_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for window_hours in (12, 24):
        rows.extend(
            [
                _row(window_hours, delta=3.0, direction_match=True, error=1.0),
                _row(window_hours, delta=-3.0, direction_match=False, error=6.0),
                _row(window_hours, delta=3.0, direction_match=True, error=1.0),
                _row(window_hours, delta=-3.0, direction_match=False, error=6.0),
                _row(window_hours, delta=3.0, direction_match=True, error=1.0),
                _row(window_hours, delta=3.0, direction_match=True, error=1.0),
                _row(window_hours, delta=-3.0, direction_match=False, error=6.0),
                _row(window_hours, delta=3.0, direction_match=True, error=1.0),
            ]
        )
    return rows


def main() -> int:
    """CLI entrypoint."""
    rows = _validation_rows()
    promote = confidence_promotion_decision(
        candidate_artifact=_artifact(coefficient=1.0, trained_at="candidate"),
        active_artifact=_artifact(coefficient=-1.0, trained_at="active"),
        validation_rows=rows,
        min_train_rows=4,
        test_fraction=0.5,
        watch_precision_target=0.7,
        max_mae_regression_pts=0.5,
    )
    regress = confidence_promotion_decision(
        candidate_artifact=_artifact(coefficient=-1.0, trained_at="candidate"),
        active_artifact=_artifact(coefficient=1.0, trained_at="active"),
        validation_rows=rows,
        min_train_rows=4,
        test_fraction=0.5,
        watch_precision_target=0.7,
        max_mae_regression_pts=0.5,
    )
    insufficient = confidence_promotion_decision(
        candidate_artifact=_artifact(coefficient=1.0, trained_at="candidate"),
        active_artifact=None,
        validation_rows=rows[:2],
        min_train_rows=4,
        test_fraction=0.5,
        watch_precision_target=0.7,
        max_mae_regression_pts=0.5,
    )
    checks = [
        {
            "name": "promotes_better_candidate",
            "ok": promote["promotion_status"] == "promoted",
            "reason": promote["promotion_reason"],
        },
        {
            "name": "rejects_accuracy_regression",
            "ok": regress["promotion_status"] == "rejected"
            and "precision" in str(regress["promotion_reason"]).lower(),
            "reason": regress["promotion_reason"],
        },
        {
            "name": "rejects_insufficient_rows",
            "ok": insufficient["promotion_status"] == "rejected"
            and "not evaluated" in str(insufficient["promotion_reason"]).lower(),
            "reason": insufficient["promotion_reason"],
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
