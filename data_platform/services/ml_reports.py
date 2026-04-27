"""Static ML report helpers for API metadata endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


WEEK10_11_REPORT_PATH = Path("data_platform/ml/WEEK10_11_RESIDUAL_MOVEMENT_REPORT.md")
CLIENT_ML_UPDATE_PATH = Path("data_platform/ml/CLIENT_ML_UPDATE.md")
FINAL_COMPARISON_JSON_PATH = Path(
    "data_platform/runtime/ml/final_week10_11_residual_model_comparison_polymarket_trade_covered.json"
)
FINAL_COMPARISON_MARKDOWN_PATH = Path(
    "data_platform/runtime/ml/final_week10_11_residual_model_comparison_polymarket_trade_covered.md"
)


def _read_text(path: Path) -> str | None:
    """Return file text when present."""
    return path.read_text(encoding="utf-8") if path.exists() else None


def _read_json(path: Path) -> dict[str, Any] | None:
    """Return JSON payload when present and well-formed."""
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else None


def _model_window_summary(model_payload: dict[str, Any], window_name: str) -> dict[str, Any]:
    """Return compact metrics for one model/window pair."""
    window = model_payload.get("windows", {}).get(window_name, {})
    return {
        "selected_config": window.get("selected_config"),
        "rmse_delta": window.get("rmse_delta"),
        "stable_whale_feature_count": window.get("stable_whale_feature_count"),
        "passing_fold_count": window.get("passing_fold_count"),
        "worsening_research_segment_count": window.get("worsening_research_segment_count"),
        "whale_lift_demonstrated": window.get("whale_lift_demonstrated"),
    }


def _comparison_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Return the stable API-facing subset of the final residual comparison."""
    if not payload:
        return {
            "available": False,
            "reason": "Final comparison JSON is not present in runtime output.",
        }

    models = payload.get("models", {})
    return {
        "available": True,
        "generated_at": payload.get("generated_at"),
        "row_count": payload.get("row_count"),
        "regime": payload.get("regime"),
        "research_segments": payload.get("research_segments"),
        "exclude_market_families": payload.get("exclude_market_families"),
        "default_estimator": payload.get("recommendation", {}).get("default_estimator"),
        "all_required_windows_lift": payload.get("recommendation", {}).get("all_required_windows_lift"),
        "window_recommendations": payload.get("recommendation", {}).get("window_recommendations"),
        "models": {
            model_name: {
                "score": model_payload.get("score"),
                "windows": {
                    "12h": _model_window_summary(model_payload, "12h"),
                    "24h": _model_window_summary(model_payload, "24h"),
                },
            }
            for model_name, model_payload in models.items()
            if isinstance(model_payload, dict)
        },
    }


def week10_11_residual_movement_report() -> dict[str, Any]:
    """Return Week 10-11 residual movement report metadata for backend consumers."""
    comparison_payload = _read_json(FINAL_COMPARISON_JSON_PATH)
    return {
        "title": "Week 10-11 Residual Whale Movement ML Report",
        "scope": {
            "market": "polymarket",
            "excluded_markets": ["kalshi"],
            "exclusion_reason": (
                "Kalshi wallet-level trader identity cannot be tracked with the same confidence, "
                "so whale trust, entry, exit, holding-time, and realized-strategy features are not reliable there."
            ),
        },
        "claim": (
            "Whale behavior improves 12h and 24h residual market-movement prediction on the larger "
            "Polymarket trade-covered dataset, with Ridge currently the most stable claim model."
        ),
        "status": "validated_research_signal",
        "production_use": False,
        "caveats": [
            "Polymarket-only scope.",
            "Trade-covered resolved-market rows only.",
            "Crypto up/down segment sensitivity changes model choice when excluded.",
            "Data coverage changes model choice on the smaller first backfill.",
            "Not production trading advice.",
        ],
        "selected_model": {
            "estimator": "ridge",
            "prediction_windows": ["12h", "24h"],
            "task": "residual_market_movement",
        },
        "source_paths": {
            "tracked_report_markdown": str(WEEK10_11_REPORT_PATH),
            "client_update_markdown": str(CLIENT_ML_UPDATE_PATH),
            "final_comparison_json": str(FINAL_COMPARISON_JSON_PATH),
            "final_comparison_markdown": str(FINAL_COMPARISON_MARKDOWN_PATH),
        },
        "comparison": _comparison_summary(comparison_payload),
        "tracked_report_markdown": _read_text(WEEK10_11_REPORT_PATH),
        "client_update_markdown": _read_text(CLIENT_ML_UPDATE_PATH),
    }
