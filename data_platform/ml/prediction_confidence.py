"""Train and apply validation-backed confidence for market-profile ML predictions."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


CONFIDENCE_MODEL_VERSION = "market_profile_prediction_confidence_v1"
DEFAULT_CONFIDENCE_MODEL_PATH = Path("data_platform/runtime/ml/market_prediction_confidence_model.json")
PREDICTION_WINDOWS = (12, 24)
FEATURE_NAMES = [
    "current_odds_pct",
    "predicted_future_odds_pct",
    "predicted_delta_pts",
    "abs_predicted_delta_pts",
    "interval_width_pts",
    "whale_total_pressure_log",
    "whale_net_pressure_log_signed",
    "pressure_ratio",
    "event_count_log",
    "trusted_event_count_log",
    "trusted_share",
    "entry_count_log",
    "exit_count_log",
    "is_up_prediction",
    "is_down_prediction",
    "is_watch",
    "is_strong",
    "is_live_whale_signal_model",
    "is_whale_anchored_report",
]
MIN_TRAIN_ROWS = 80
MIN_TEST_ROWS = 20
WATCH_PRECISION_TARGET = 0.70
STRONG_PRECISION_TARGET = 0.80

VALIDATED_CONFIDENCE_ROWS_SQL = text(
    """
    SELECT
      v.ml_market_prediction_validation_id,
      v.prediction_window_hours,
      v.prediction_generated_at,
      v.predicted_delta_pts,
      v.actual_delta_pts,
      v.predicted_direction,
      v.actual_direction,
      v.direction_match,
      v.absolute_error_pts,
      v.validation_status,
      s.current_odds_pct,
      s.predicted_future_odds_pct,
      s.signal_tier,
      s.display_tier,
      s.prediction_source,
      s.prediction_payload,
      s.reliability_payload
    FROM analytics.ml_market_prediction_validation v
    JOIN analytics.ml_market_prediction_snapshot s
      ON s.ml_market_prediction_snapshot_id = v.ml_market_prediction_snapshot_id
    JOIN analytics.platform p
      ON p.platform_id = s.platform_id
    WHERE p.platform_name = :platform_name
      AND v.validation_status = 'validated'
      AND v.direction_match IS NOT NULL
      AND v.predicted_direction IN ('up', 'down')
    ORDER BY v.prediction_generated_at ASC, v.ml_market_prediction_validation_id ASC
    """
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _prediction_window(payload: dict[str, Any], row: dict[str, Any] | None = None) -> int:
    raw = payload.get("prediction_window_hours")
    if raw is None and row:
        raw = row.get("prediction_window_hours")
    if raw is None:
        raw = str(payload.get("window") or "").replace("h", "")
    return _safe_int(raw)


def _feature_vector_from_payload(payload: dict[str, Any], row: dict[str, Any] | None = None) -> list[float]:
    row = row or {}
    whale_anchor = payload.get("whale_anchor") if isinstance(payload.get("whale_anchor"), dict) else {}
    current = _safe_float(payload.get("current_odds_pct", row.get("current_odds_pct")))
    future = _safe_float(payload.get("predicted_future_odds_pct", row.get("predicted_future_odds_pct")), current)
    delta = _safe_float(payload.get("predicted_delta_pts", row.get("predicted_delta_pts")), future - current)
    interval_low = _safe_float(payload.get("interval_low_future_odds_pct"), future)
    interval_high = _safe_float(payload.get("interval_high_future_odds_pct"), future)
    interval_width = abs(interval_high - interval_low)
    total_pressure = _safe_float(whale_anchor.get("side_total_pressure"))
    net_pressure = _safe_float(whale_anchor.get("side_net_pressure"))
    event_count = _safe_int(whale_anchor.get("event_count"))
    trusted_event_count = _safe_int(whale_anchor.get("trusted_event_count"))
    trusted_share = trusted_event_count / max(event_count, 1)
    entry_count = _safe_int(whale_anchor.get("recent_entry_count_12h"))
    exit_count = _safe_int(whale_anchor.get("recent_exit_count_12h"))
    predicted_direction = str(payload.get("predicted_direction") or row.get("predicted_direction") or "").lower()
    signal_tier = str(payload.get("direction_signal_tier") or row.get("signal_tier") or "").lower()
    prediction_source = str(payload.get("prediction_source") or row.get("prediction_source") or "").lower()
    return [
        current,
        future,
        delta,
        abs(delta),
        interval_width,
        math.log1p(max(total_pressure, 0.0)),
        math.copysign(math.log1p(abs(net_pressure)), net_pressure),
        _safe_float(whale_anchor.get("pressure_ratio")),
        math.log1p(max(event_count, 0)),
        math.log1p(max(trusted_event_count, 0)),
        max(0.0, min(trusted_share, 1.0)),
        math.log1p(max(entry_count, 0)),
        math.log1p(max(exit_count, 0)),
        1.0 if predicted_direction == "up" else 0.0,
        1.0 if predicted_direction == "down" else 0.0,
        1.0 if signal_tier == "watch" else 0.0,
        1.0 if signal_tier == "strong" else 0.0,
        1.0 if prediction_source == "live_whale_signal_model" else 0.0,
        1.0 if prediction_source == "whale_anchored_report" else 0.0,
    ]


def _ece(labels: list[int], probabilities: list[float], bins: int = 10) -> float | None:
    if not labels:
        return None
    total = len(labels)
    error = 0.0
    for index in range(bins):
        low = index / bins
        high = (index + 1) / bins
        members = [
            (label, probability)
            for label, probability in zip(labels, probabilities)
            if (low <= probability < high) or (index == bins - 1 and probability == high)
        ]
        if not members:
            continue
        accuracy = sum(label for label, _ in members) / len(members)
        confidence = sum(probability for _, probability in members) / len(members)
        error += len(members) / total * abs(accuracy - confidence)
    return error


def _select_threshold(
    labels: list[int],
    probabilities: list[float],
    *,
    target_precision: float,
    fallback: float,
) -> dict[str, Any]:
    if not labels:
        return {
            "threshold": fallback,
            "precision": None,
            "coverage": 0.0,
            "target_precision": target_precision,
            "selection": "fallback_no_holdout_rows",
        }
    best: dict[str, Any] | None = None
    for step in range(50, 96):
        threshold = step / 100
        selected = [(label, probability) for label, probability in zip(labels, probabilities) if probability >= threshold]
        if not selected:
            continue
        precision = sum(label for label, _ in selected) / len(selected)
        coverage = len(selected) / len(labels)
        if precision >= target_precision and (best is None or coverage > best["coverage"]):
            best = {
                "threshold": threshold,
                "precision": precision,
                "coverage": coverage,
                "target_precision": target_precision,
                "selection": "holdout_precision_target",
            }
    if best:
        return best
    return {
        "threshold": fallback,
        "precision": None,
        "coverage": 0.0,
        "target_precision": target_precision,
        "selection": "fallback_no_threshold_met_target",
    }


def _error_bins(labels: list[int], probabilities: list[float], absolute_errors: list[float]) -> list[dict[str, Any]]:
    bins: list[dict[str, Any]] = []
    for index in range(10):
        low = index / 10
        high = (index + 1) / 10
        members = [
            (label, probability, error)
            for label, probability, error in zip(labels, probabilities, absolute_errors)
            if (low <= probability < high) or (index == 9 and probability == high)
        ]
        if not members:
            continue
        bins.append(
            {
                "min_confidence": round(low, 2),
                "max_confidence": round(high, 2),
                "row_count": len(members),
                "direction_match_pct": round(100.0 * sum(label for label, _, _ in members) / len(members), 2),
                "mean_absolute_error_pts": round(sum(error for _, _, error in members) / len(members), 4),
            }
        )
    return bins


def load_validated_confidence_rows(session: Session, *, platform_name: str = "polymarket") -> list[dict[str, Any]]:
    """Return validated prediction rows used for confidence training and promotion checks."""
    return [dict(row) for row in session.execute(VALIDATED_CONFIDENCE_ROWS_SQL, {"platform_name": platform_name}).mappings()]


def _split_index(rows: list[dict[str, Any]], *, min_train_rows: int, test_fraction: float) -> int:
    split_index = max(min_train_rows, int(len(rows) * (1.0 - test_fraction)))
    if len(rows) - split_index < MIN_TEST_ROWS and len(rows) >= min_train_rows + MIN_TEST_ROWS:
        split_index = len(rows) - MIN_TEST_ROWS
    return min(max(split_index, 1), len(rows))


def _holdout_rows(rows: list[dict[str, Any]], *, min_train_rows: int, test_fraction: float) -> list[dict[str, Any]]:
    if len(rows) < min_train_rows:
        return []
    split_index = _split_index(rows, min_train_rows=min_train_rows, test_fraction=test_fraction)
    return rows[split_index:] or rows[:split_index]


def _train_window_model(
    rows: list[dict[str, Any]],
    *,
    min_train_rows: int,
    test_fraction: float,
    watch_precision_target: float,
    strong_precision_target: float,
) -> dict[str, Any]:
    if len(rows) < min_train_rows:
        return {"status": "insufficient_rows", "row_count": len(rows), "min_train_rows": min_train_rows}
    labels = [1 if bool(row.get("direction_match")) else 0 for row in rows]
    if len(set(labels)) < 2:
        return {"status": "insufficient_label_classes", "row_count": len(rows), "positive_rows": sum(labels)}

    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, brier_score_loss
    from sklearn.preprocessing import StandardScaler

    vectors = [_feature_vector_from_payload(_payload_dict(row.get("prediction_payload")), row) for row in rows]
    split_index = _split_index(rows, min_train_rows=min_train_rows, test_fraction=test_fraction)
    train_vectors = vectors[:split_index]
    train_labels = labels[:split_index]
    test_vectors = vectors[split_index:] or vectors[:split_index]
    test_labels = labels[split_index:] or labels[:split_index]
    test_errors = [_safe_float(row.get("absolute_error_pts")) for row in (rows[split_index:] or rows[:split_index])]

    scaler = StandardScaler()
    train_scaled = scaler.fit_transform(train_vectors)
    test_scaled = scaler.transform(test_vectors)
    model = LogisticRegression(class_weight="balanced", max_iter=1000, random_state=470)
    model.fit(train_scaled, train_labels)
    test_probabilities = [float(value) for value in model.predict_proba(test_scaled)[:, 1]]
    test_predictions = [1 if probability >= 0.5 else 0 for probability in test_probabilities]
    ece = _ece(test_labels, test_probabilities)
    watch_threshold = _select_threshold(
        test_labels,
        test_probabilities,
        target_precision=watch_precision_target,
        fallback=0.7,
    )
    strong_threshold = _select_threshold(
        test_labels,
        test_probabilities,
        target_precision=strong_precision_target,
        fallback=0.8,
    )

    return {
        "status": "trained",
        "row_count": len(rows),
        "train_rows": len(train_labels),
        "test_rows": len(test_labels),
        "feature_names": FEATURE_NAMES,
        "scaler_mean": [float(value) for value in scaler.mean_],
        "scaler_scale": [float(value) if float(value) != 0 else 1.0 for value in scaler.scale_],
        "coefficients": [float(value) for value in model.coef_[0]],
        "intercept": float(model.intercept_[0]),
        "thresholds": {
            "watch": watch_threshold,
            "strong": strong_threshold,
        },
        "metrics": {
            "holdout_accuracy_pct": round(100.0 * float(accuracy_score(test_labels, test_predictions)), 2),
            "holdout_brier": round(float(brier_score_loss(test_labels, test_probabilities)), 6),
            "holdout_ece_pct": round(100.0 * ece, 2) if ece is not None else None,
            "holdout_direction_match_pct": round(100.0 * sum(test_labels) / len(test_labels), 2),
            "holdout_mean_absolute_error_pts": round(sum(test_errors) / len(test_errors), 4) if test_errors else None,
            "holdout_rmse_pts": round(math.sqrt(sum(error * error for error in test_errors) / len(test_errors)), 4)
            if test_errors
            else None,
        },
        "error_bins": _error_bins(test_labels, test_probabilities, test_errors),
    }


def train_prediction_confidence_model(
    session: Session,
    *,
    platform_name: str = "polymarket",
    min_train_rows: int = MIN_TRAIN_ROWS,
    test_fraction: float = 0.25,
    watch_precision_target: float = WATCH_PRECISION_TARGET,
    strong_precision_target: float = STRONG_PRECISION_TARGET,
) -> dict[str, Any]:
    """Train per-window confidence models from validated prediction outcomes."""
    raw_rows = load_validated_confidence_rows(session, platform_name=platform_name)
    windows: dict[str, Any] = {}
    for window_hours in PREDICTION_WINDOWS:
        window_rows = [row for row in raw_rows if _safe_int(row.get("prediction_window_hours")) == window_hours]
        windows[str(window_hours)] = _train_window_model(
            window_rows,
            min_train_rows=max(min_train_rows, 1),
            test_fraction=max(0.05, min(test_fraction, 0.5)),
            watch_precision_target=max(0.0, min(float(watch_precision_target), 1.0)),
            strong_precision_target=max(0.0, min(float(strong_precision_target), 1.0)),
        )
    return {
        "model_version": CONFIDENCE_MODEL_VERSION,
        "platform": platform_name,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_source": "analytics.ml_market_prediction_validation_joined_to_snapshots",
        "label": "direction_match_on_validated_12h_24h_predictions",
        "row_count": len(raw_rows),
        "min_train_rows": min_train_rows,
        "test_fraction": test_fraction,
        "watch_precision_target": watch_precision_target,
        "strong_precision_target": strong_precision_target,
        "windows": windows,
    }


def write_confidence_artifact(artifact: dict[str, Any], output_path: Path) -> None:
    """Write a confidence artifact as JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8")


def load_confidence_artifact(path: Path | str | None = None) -> dict[str, Any] | None:
    """Load a confidence artifact if present and readable."""
    if not path:
        return None
    artifact_path = Path(path)
    if not artifact_path.exists():
        return None
    try:
        parsed = json.loads(artifact_path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _expected_error_for_score(window_model: dict[str, Any], score: float) -> float | None:
    for bin_row in window_model.get("error_bins") or []:
        low = _safe_float(bin_row.get("min_confidence"))
        high = _safe_float(bin_row.get("max_confidence"), 1.0)
        if low <= score < high or (score == 1.0 and high == 1.0):
            return _safe_float(bin_row.get("mean_absolute_error_pts"))
    metric = window_model.get("metrics") if isinstance(window_model.get("metrics"), dict) else {}
    value = metric.get("holdout_mean_absolute_error_pts")
    return _safe_float(value) if value is not None else None


def _bin_accuracy_for_score(window_model: dict[str, Any], score: float) -> float | None:
    """Return empirical validation accuracy for the score bucket, as a 0-100 percentage."""
    for bin_row in window_model.get("error_bins") or []:
        low = _safe_float(bin_row.get("min_confidence"))
        high = _safe_float(bin_row.get("max_confidence"), 1.0)
        if low <= score < high or (score == 1.0 and high == 1.0):
            value = bin_row.get("direction_match_pct")
            return _safe_float(value) if value is not None else None
    return None


def _threshold_precision_pct(thresholds: dict[str, Any], tier: str) -> float | None:
    """Return holdout precision for a selected tier threshold, as a 0-100 percentage."""
    threshold_detail = thresholds.get(tier) if isinstance(thresholds.get(tier), dict) else {}
    precision = threshold_detail.get("precision")
    return round(100.0 * _safe_float(precision), 2) if precision is not None else None


def score_prediction_confidence(
    payload: dict[str, Any],
    artifact: dict[str, Any] | None,
    row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the trained confidence score for one prediction payload."""
    if not artifact:
        return {"available": False, "reason": "artifact_missing"}
    window_hours = _prediction_window(payload, row)
    window_model = (artifact.get("windows") or {}).get(str(window_hours))
    if not isinstance(window_model, dict) or window_model.get("status") != "trained":
        return {"available": False, "reason": "window_not_trained", "window_hours": window_hours}

    vector = _feature_vector_from_payload(payload, row)
    means = [_safe_float(value) for value in window_model.get("scaler_mean") or []]
    scales = [_safe_float(value, 1.0) or 1.0 for value in window_model.get("scaler_scale") or []]
    coefficients = [_safe_float(value) for value in window_model.get("coefficients") or []]
    if len(vector) != len(means) or len(vector) != len(scales) or len(vector) != len(coefficients):
        return {"available": False, "reason": "feature_schema_mismatch", "window_hours": window_hours}

    linear_score = _safe_float(window_model.get("intercept"))
    for value, mean, scale, coefficient in zip(vector, means, scales, coefficients):
        linear_score += ((value - mean) / scale) * coefficient
    confidence = _sigmoid(linear_score)
    return {
        "available": True,
        "score": confidence,
        "window_hours": window_hours,
        "window_model": window_model,
    }


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _evaluation_summary(
    rows: list[dict[str, Any]],
    artifact: dict[str, Any] | None,
    *,
    window_hours: int,
    min_train_rows: int,
    test_fraction: float,
) -> dict[str, Any]:
    window_rows = [row for row in rows if _safe_int(row.get("prediction_window_hours")) == window_hours]
    holdout_rows = _holdout_rows(
        window_rows,
        min_train_rows=max(min_train_rows, 1),
        test_fraction=max(0.05, min(test_fraction, 0.5)),
    )
    if not holdout_rows:
        return {
            "status": "insufficient_rows",
            "window_hours": window_hours,
            "row_count": len(window_rows),
            "holdout_rows": 0,
        }

    scored: list[dict[str, Any]] = []
    unscored_count = 0
    window_model: dict[str, Any] | None = None
    for row in holdout_rows:
        payload = _payload_dict(row.get("prediction_payload"))
        score_detail = score_prediction_confidence(payload, artifact, row)
        if not score_detail.get("available"):
            unscored_count += 1
            continue
        window_model = score_detail.get("window_model") if isinstance(score_detail.get("window_model"), dict) else window_model
        scored.append(
            {
                "score": _safe_float(score_detail.get("score")),
                "label": 1 if bool(row.get("direction_match")) else 0,
                "absolute_error_pts": _safe_float(row.get("absolute_error_pts")),
                "predicted_direction": str(row.get("predicted_direction") or "").lower(),
            }
        )

    if not scored or window_model is None:
        return {
            "status": "unscored",
            "window_hours": window_hours,
            "row_count": len(window_rows),
            "holdout_rows": len(holdout_rows),
            "scored_rows": 0,
            "unscored_rows": unscored_count,
        }

    thresholds = window_model.get("thresholds") if isinstance(window_model.get("thresholds"), dict) else {}
    watch_threshold = _safe_float((thresholds.get("watch") or {}).get("threshold"), 0.7)
    strong_threshold = _safe_float((thresholds.get("strong") or {}).get("threshold"), 0.8)
    directional = [item for item in scored if item["predicted_direction"] in {"up", "down"}]
    watch_rows = [item for item in directional if item["score"] >= watch_threshold]
    strong_rows = [item for item in directional if item["score"] >= strong_threshold]
    score_predictions = [1 if item["score"] >= 0.5 else 0 for item in scored]
    labels = [int(item["label"]) for item in scored]
    errors = [float(item["absolute_error_pts"]) for item in scored]
    watch_errors = [float(item["absolute_error_pts"]) for item in watch_rows]
    strong_errors = [float(item["absolute_error_pts"]) for item in strong_rows]
    return {
        "status": "evaluated",
        "window_hours": window_hours,
        "row_count": len(window_rows),
        "holdout_rows": len(holdout_rows),
        "scored_rows": len(scored),
        "unscored_rows": unscored_count,
        "watch_threshold": round(watch_threshold, 4),
        "strong_threshold": round(strong_threshold, 4),
        "direction_accuracy_pct": round(100.0 * sum(1 for label, prediction in zip(labels, score_predictions) if label == prediction) / len(scored), 2),
        "direction_match_base_rate_pct": round(100.0 * sum(labels) / len(labels), 2),
        "mae_pts": round(_mean(errors) or 0.0, 4),
        "rmse_pts": round(math.sqrt(sum(error * error for error in errors) / len(errors)), 4) if errors else None,
        "watch_signal_count": len(watch_rows),
        "watch_precision_pct": round(100.0 * sum(item["label"] for item in watch_rows) / len(watch_rows), 2) if watch_rows else None,
        "watch_coverage_pct": round(100.0 * len(watch_rows) / len(scored), 2),
        "watch_mae_pts": round(_mean(watch_errors), 4) if watch_errors else None,
        "watch_rmse_pts": round(math.sqrt(sum(error * error for error in watch_errors) / len(watch_errors)), 4)
        if watch_errors
        else None,
        "strong_signal_count": len(strong_rows),
        "strong_precision_pct": round(100.0 * sum(item["label"] for item in strong_rows) / len(strong_rows), 2) if strong_rows else None,
        "strong_coverage_pct": round(100.0 * len(strong_rows) / len(scored), 2),
        "strong_mae_pts": round(_mean(strong_errors), 4) if strong_errors else None,
        "strong_rmse_pts": round(math.sqrt(sum(error * error for error in strong_errors) / len(strong_errors)), 4)
        if strong_errors
        else None,
    }


def evaluate_confidence_artifact(
    rows: list[dict[str, Any]],
    artifact: dict[str, Any] | None,
    *,
    min_train_rows: int = MIN_TRAIN_ROWS,
    test_fraction: float = 0.25,
) -> dict[str, Any]:
    """Evaluate an artifact on the same chronological holdout used for promotion."""
    windows = {
        str(window_hours): _evaluation_summary(
            rows,
            artifact,
            window_hours=window_hours,
            min_train_rows=min_train_rows,
            test_fraction=test_fraction,
        )
        for window_hours in PREDICTION_WINDOWS
    }
    evaluated = [window for window in windows.values() if window.get("status") == "evaluated"]
    watch_precisions = [
        _safe_float(window.get("watch_precision_pct"))
        for window in evaluated
        if window.get("watch_precision_pct") is not None
    ]
    watch_maes = [_safe_float(window.get("watch_mae_pts")) for window in evaluated if window.get("watch_mae_pts") is not None]
    return {
        "status": "evaluated" if evaluated else "unscored",
        "model_version": artifact.get("model_version") if artifact else None,
        "trained_at": artifact.get("trained_at") if artifact else None,
        "windows": windows,
        "average_watch_precision_pct": round(_mean(watch_precisions), 2) if watch_precisions else None,
        "average_watch_mae_pts": round(_mean(watch_maes), 4) if watch_maes else None,
    }


def confidence_promotion_decision(
    *,
    candidate_artifact: dict[str, Any],
    active_artifact: dict[str, Any] | None,
    validation_rows: list[dict[str, Any]],
    min_train_rows: int = MIN_TRAIN_ROWS,
    test_fraction: float = 0.25,
    watch_precision_target: float = WATCH_PRECISION_TARGET,
    max_mae_regression_pts: float = 0.5,
) -> dict[str, Any]:
    """Return a gated promotion decision for a candidate confidence artifact."""
    candidate_metrics = evaluate_confidence_artifact(
        validation_rows,
        candidate_artifact,
        min_train_rows=min_train_rows,
        test_fraction=test_fraction,
    )
    active_metrics = evaluate_confidence_artifact(
        validation_rows,
        active_artifact,
        min_train_rows=min_train_rows,
        test_fraction=test_fraction,
    ) if active_artifact else {"status": "missing", "windows": {}}

    target_pct = 100.0 * max(0.0, min(float(watch_precision_target), 1.0))
    reasons: list[str] = []
    for window_hours in PREDICTION_WINDOWS:
        key = str(window_hours)
        candidate_window = candidate_metrics.get("windows", {}).get(key, {})
        active_window = active_metrics.get("windows", {}).get(key, {}) if active_metrics else {}
        if candidate_window.get("status") != "evaluated":
            reasons.append(f"{window_hours}h candidate not evaluated")
            continue
        if int(candidate_window.get("watch_signal_count") or 0) <= 0:
            reasons.append(f"{window_hours}h candidate produced no Watch/Strong holdout signals")
            continue
        candidate_precision = candidate_window.get("watch_precision_pct")
        if candidate_precision is None or float(candidate_precision) < target_pct:
            reasons.append(f"{window_hours}h candidate Watch precision below {round(target_pct, 2)}%")
            continue
        if active_window.get("status") != "evaluated" or active_window.get("watch_precision_pct") is None:
            continue
        active_precision = float(active_window.get("watch_precision_pct") or 0.0)
        if float(candidate_precision) + 1e-9 < active_precision:
            reasons.append(f"{window_hours}h candidate Watch precision regressed below active model")
            continue
        candidate_mae = candidate_window.get("watch_mae_pts")
        active_mae = active_window.get("watch_mae_pts")
        if candidate_mae is not None and active_mae is not None:
            if float(candidate_mae) > float(active_mae) + max(0.0, float(max_mae_regression_pts)):
                reasons.append(f"{window_hours}h candidate Watch MAE regressed beyond tolerance")

    promoted = not reasons
    return {
        "promotion_status": "promoted" if promoted else "rejected",
        "promotion_reason": "candidate passed gated holdout promotion checks" if promoted else "; ".join(reasons),
        "candidate_metrics": candidate_metrics,
        "active_metrics": active_metrics,
        "previous_model_trained_at": active_artifact.get("trained_at") if active_artifact else None,
        "watch_precision_target": watch_precision_target,
        "max_mae_regression_pts": max_mae_regression_pts,
    }


def apply_trained_confidence(payload: dict[str, Any], artifact: dict[str, Any] | None) -> dict[str, Any]:
    """Annotate one prediction payload with trained confidence when an artifact is available."""
    if not artifact:
        payload["trained_confidence_available"] = False
        payload["confidence_source"] = "heuristic_rules_no_trained_artifact"
        return payload
    window_hours = _prediction_window(payload)
    window_model = (artifact.get("windows") or {}).get(str(window_hours))
    if not isinstance(window_model, dict) or window_model.get("status") != "trained":
        payload["trained_confidence_available"] = False
        payload["confidence_source"] = "heuristic_rules_window_not_trained"
        return payload

    vector = _feature_vector_from_payload(payload)
    means = [_safe_float(value) for value in window_model.get("scaler_mean") or []]
    scales = [_safe_float(value, 1.0) or 1.0 for value in window_model.get("scaler_scale") or []]
    coefficients = [_safe_float(value) for value in window_model.get("coefficients") or []]
    if len(vector) != len(means) or len(vector) != len(scales) or len(vector) != len(coefficients):
        payload["trained_confidence_available"] = False
        payload["confidence_source"] = "heuristic_rules_feature_schema_mismatch"
        return payload

    linear_score = _safe_float(window_model.get("intercept"))
    for value, mean, scale, coefficient in zip(vector, means, scales, coefficients):
        linear_score += ((value - mean) / scale) * coefficient
    confidence = _sigmoid(linear_score)
    thresholds = window_model.get("thresholds") if isinstance(window_model.get("thresholds"), dict) else {}
    watch_threshold = _safe_float((thresholds.get("watch") or {}).get("threshold"), 0.7)
    strong_threshold = _safe_float((thresholds.get("strong") or {}).get("threshold"), 0.8)
    expected_error = _expected_error_for_score(window_model, confidence)
    bin_accuracy_pct = _bin_accuracy_for_score(window_model, confidence)
    metrics = window_model.get("metrics") if isinstance(window_model.get("metrics"), dict) else {}
    predicted_direction = str(payload.get("predicted_direction") or "").lower()

    payload["trained_confidence_available"] = True
    payload["confidence_source"] = "trained_closed_market_validation_model"
    payload["trained_confidence_model_version"] = artifact.get("model_version")
    payload["trained_confidence_trained_at"] = artifact.get("trained_at")
    payload["trained_confidence_score"] = round(confidence, 4)
    payload["trained_confidence_pct"] = round(confidence * 100.0, 2)
    payload["model_confidence_score"] = round(confidence, 4)
    payload["model_confidence_pct"] = round(confidence * 100.0, 2)
    payload["confidence_training_window_rows"] = window_model.get("row_count")
    payload["confidence_holdout_direction_match_pct"] = metrics.get("holdout_direction_match_pct")
    payload["confidence_calibration_ece_pct"] = metrics.get("holdout_ece_pct")
    payload["expected_direction_error_pts"] = round(expected_error, 4) if expected_error is not None else None
    payload["direction_signal_confidence"] = round(confidence, 4)

    warnings = list(payload.get("reliability_warnings") or [])
    display_reasons = list(payload.get("display_reasons") or [])
    review_reasons = list(payload.get("review_reasons") or [])
    if predicted_direction not in {"up", "down"}:
        payload["direction_signal_tier"] = "abstain"
        payload["display_tier"] = "review"
        payload["historical_validation_tier"] = "trained_flat_signal"
        payload["historical_validation_reason"] = "trained confidence is only used for non-flat up/down forecasts"
        display_accuracy_pct = bin_accuracy_pct
        accuracy_source = "validation_bucket_accuracy"
        if "flat_prediction" not in warnings:
            warnings.append("flat_prediction")
    elif confidence >= strong_threshold:
        payload["direction_signal_tier"] = "strong"
        payload["display_tier"] = "show"
        payload["historical_validation_tier"] = "trained_strong_confidence"
        payload["historical_validation_reason"] = "closed-market validation model rates this as a strongest-confidence direction forecast"
        payload["direction_signal_tier_reason"] = "trained closed-market validation confidence reached the Strong threshold"
        review_reasons = []
        if "trained_confidence_model" not in display_reasons:
            display_reasons.append("trained_confidence_model")
        display_accuracy_pct = _threshold_precision_pct(thresholds, "strong") or bin_accuracy_pct
        accuracy_source = "strong_threshold_holdout_precision"
    elif confidence >= watch_threshold:
        payload["direction_signal_tier"] = "watch"
        payload["display_tier"] = "show"
        payload["historical_validation_tier"] = "trained_watch_confidence"
        payload["historical_validation_reason"] = "closed-market validation model rates this as a Watch signal; validate against realized 12h/24h movement"
        payload["direction_signal_tier_reason"] = "trained closed-market validation confidence reached the Watch threshold"
        review_reasons = []
        if "trained_confidence_model" not in display_reasons:
            display_reasons.append("trained_confidence_model")
        display_accuracy_pct = _threshold_precision_pct(thresholds, "watch") or bin_accuracy_pct
        accuracy_source = "watch_threshold_holdout_precision"
    else:
        payload["direction_signal_tier"] = "abstain"
        payload["display_tier"] = "review"
        payload["historical_validation_tier"] = "trained_low_confidence"
        payload["historical_validation_reason"] = "closed-market validation model does not rate this direction forecast as reliable yet"
        payload["direction_signal_tier_reason"] = "trained confidence is below the Watch threshold"
        if "trained_low_confidence" not in warnings:
            warnings.append("trained_low_confidence")
        review_reasons = ["trained_low_confidence"]
        display_accuracy_pct = bin_accuracy_pct
        accuracy_source = "validation_bucket_accuracy"

    if display_accuracy_pct is None:
        display_accuracy_pct = metrics.get("holdout_direction_match_pct")
        accuracy_source = "window_holdout_direction_match"
    payload["validation_accuracy_pct"] = round(_safe_float(display_accuracy_pct), 2) if display_accuracy_pct is not None else None
    payload["direction_signal_accuracy_pct"] = payload["validation_accuracy_pct"]
    payload["accuracy_source"] = accuracy_source
    payload["historical_validation_direction_match_pct"] = payload["validation_accuracy_pct"]
    payload["historical_validation_sample_size"] = window_model.get("row_count")
    payload["reliability_warnings"] = warnings
    payload["display_reasons"] = display_reasons
    payload["review_reasons"] = review_reasons
    return payload
