"""Validate focused whale trend-magnitude optimization guardrails."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


REPORT_PATH = Path("data_platform/ml/ML_WHALE_ANCHORED_DELTA_CURRENT_DB_ASOF.json")
WINDOWS = ("12h", "24h")
PROTECTED_CATEGORIES = ("crypto", "politics", "world_geopolitics", "technology")


def _read_report() -> dict[str, Any]:
    if not REPORT_PATH.exists():
        return {}
    payload = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _summary_by_category(window: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = window.get("focused_category_fit_summary") or []
    return {
        str(row.get("category")): row
        for row in rows
        if isinstance(row, dict)
    }


def _ok_no_regression(row: dict[str, Any]) -> bool:
    return (
        float(row.get("mae_delta_pts") or 0.0) <= 0.0001
        and float(row.get("direction_match_pct") or 0.0)
        >= float(row.get("pre_calibration_direction_match_pct") or 0.0)
    )


def main() -> int:
    """CLI entrypoint."""
    report = _read_report()
    windows = report.get("windows") or {}
    profile_index = report.get("market_profile_predictions") or {}
    by_market = profile_index.get("by_market_slug") or {}
    checks: list[dict[str, Any]] = []

    checks.append(
        {
            "name": "market_profile_prediction_index_present",
            "ok": bool(profile_index.get("local_backtest_only"))
            and int(profile_index.get("market_count") or 0) > 0
            and int(profile_index.get("prediction_count") or 0) > 0
            and isinstance(by_market, dict)
            and bool(by_market),
            "market_count": profile_index.get("market_count"),
            "prediction_count": profile_index.get("prediction_count"),
        }
    )

    for window_name in WINDOWS:
        window = windows.get(window_name) or {}
        categories = _summary_by_category(window)
        diagnostics = ((window.get("trend_fit_diagnostics") or {}).get("focused_category") or [])
        diagnostic_categories = {
            str(row.get("segment")): row
            for row in diagnostics
            if isinstance(row, dict)
        }
        trajectory = window.get("trajectory_fit_summary") or {}
        crypto_audit = window.get("crypto_direction_miss_audit") or {}
        crypto_split = window.get("crypto_direction_split_diagnostics") or {}
        crypto_absolute_audit = window.get("crypto_absolute_move_review_audit") or {}
        crypto_segment_gate = window.get("crypto_segment_direction_gate") or {}
        crypto_source_selector = window.get("crypto_direction_source_selector") or {}
        crypto_promoted_audit = window.get("crypto_promoted_row_precision_audit") or {}
        whale_timing = window.get("whale_timing_direction_diagnostics") or {}
        absolute_gate = window.get("absolute_move_display_gate") or {}
        absolute_policy = absolute_gate.get("policy") or {}
        max_review_direction_regression = float(absolute_policy.get("review_max_direction_regression_pts") or 0.0)
        policy = window.get("selected_trend_fit_policy") or {}
        candidate_backtests = window.get("calibration_candidate_backtests") or []

        checks.append(
            {
                "name": f"{window_name}_trend_fit_diagnostics_present",
                "ok": all(category in diagnostic_categories for category in PROTECTED_CATEGORIES),
                "categories": sorted(diagnostic_categories),
            }
        )
        checks.append(
            {
                "name": f"{window_name}_trajectory_fit_summary_present",
                "ok": int(trajectory.get("row_count") or 0) > 0
                and "average_path_mae_pts" in trajectory
                and "signed_area_error_pts" in trajectory,
                "trajectory": trajectory,
            }
        )
        checks.append(
            {
                "name": f"{window_name}_calibration_candidate_policy_present",
                "ok": bool(candidate_backtests)
                and {
                    "identity",
                    "magnitude_scale",
                    "direction_conditioned_magnitude",
                    "signed_bias",
                    "slope_intercept",
                }.issubset(set(policy.get("candidate_methods") or [])),
                "candidate_backtest_count": len(candidate_backtests),
                "candidate_methods": policy.get("candidate_methods"),
            }
        )
        checks.append(
            {
                "name": f"{window_name}_crypto_direction_miss_audit_present",
                "ok": int(crypto_audit.get("row_count") or 0) > 0
                and bool(crypto_audit.get("summary"))
                and bool(crypto_audit.get("by_time_to_close_bucket")),
                "row_count": crypto_audit.get("row_count"),
            }
        )
        checks.append(
            {
                "name": f"{window_name}_crypto_direction_split_diagnostics_present",
                "ok": int(crypto_split.get("row_count") or 0) > 0
                and bool(crypto_split.get("summary"))
                and bool(crypto_split.get("by_crypto_asset"))
                and bool(crypto_split.get("by_market_family"))
                and bool(crypto_split.get("by_entry_timing_bucket")),
                "row_count": crypto_split.get("row_count"),
            }
        )
        checks.append(
            {
                "name": f"{window_name}_whale_timing_direction_diagnostics_present",
                "ok": int(whale_timing.get("row_count") or 0) > 0
                and bool(whale_timing.get("by_entry_timing_bucket"))
                and bool(whale_timing.get("by_flow_timing_bucket"))
                and bool(whale_timing.get("crypto_by_entry_timing_bucket")),
                "row_count": whale_timing.get("row_count"),
            }
        )
        promoted_count = int(crypto_segment_gate.get("promoted_row_count") or 0)
        kept_review_count = int(crypto_segment_gate.get("kept_review_row_count") or 0)
        eligible_review_count = int(crypto_segment_gate.get("eligible_review_row_count") or 0)
        promoted_rows = crypto_segment_gate.get("promoted_rows") or {}
        checks.append(
            {
                "name": f"{window_name}_crypto_segment_direction_gate_present",
                "ok": int(crypto_segment_gate.get("row_count") or 0) > 0
                and promoted_count + kept_review_count == eligible_review_count
                and int(crypto_segment_gate.get("promoted_stale_btc_row_count") or 0) == 0
                and bool(crypto_segment_gate.get("policy")),
                "row_count": crypto_segment_gate.get("row_count"),
                "promoted_row_count": promoted_count,
                "kept_review_row_count": kept_review_count,
                "eligible_review_row_count": eligible_review_count,
                "promoted_stale_btc_row_count": crypto_segment_gate.get("promoted_stale_btc_row_count"),
            }
        )
        if promoted_count > 0:
            checks.append(
                {
                    "name": f"{window_name}_crypto_segment_direction_gate_promoted_rows_safe",
                    "ok": float(promoted_rows.get("mae_delta_pts") or 0.0) <= 0.0001
                    and float(promoted_rows.get("direction_match_delta_pts") or 0.0) >= -0.0001,
                    "mae_delta_pts": promoted_rows.get("mae_delta_pts"),
                    "direction_match_delta_pts": promoted_rows.get("direction_match_delta_pts"),
                }
            )
            checks.append(
                {
                    "name": f"{window_name}_crypto_promoted_precision_audit_present",
                    "ok": int(crypto_promoted_audit.get("row_count") or 0) == promoted_count
                    and "false_show_count" in crypto_promoted_audit
                    and bool(crypto_promoted_audit.get("false_show_examples") is not None),
                    "row_count": crypto_promoted_audit.get("row_count"),
                    "precision_pct": crypto_promoted_audit.get("precision_pct"),
                    "false_show_count": crypto_promoted_audit.get("false_show_count"),
                }
            )
        checks.append(
            {
                "name": f"{window_name}_crypto_direction_source_selector_present",
                "ok": int(crypto_source_selector.get("row_count") or 0) > 0
                and bool(crypto_source_selector.get("policy"))
                and "source_counts" in crypto_source_selector
                and "reason_counts" in crypto_source_selector,
                "row_count": crypto_source_selector.get("row_count"),
                "applied_row_count": crypto_source_selector.get("applied_row_count"),
            }
        )
        if int(crypto_source_selector.get("applied_row_count") or 0) > 0:
            selector_applied = crypto_source_selector.get("applied_rows") or {}
            checks.append(
                {
                    "name": f"{window_name}_crypto_direction_source_selector_applied_rows_safe",
                    "ok": float(selector_applied.get("mae_delta_pts") or 0.0) <= 0.0001
                    and float(selector_applied.get("direction_delta_pts") or 0.0) >= -0.0001,
                    "mae_delta_pts": selector_applied.get("mae_delta_pts"),
                    "direction_delta_pts": selector_applied.get("direction_delta_pts"),
                }
            )
        crypto_absolute_summary = crypto_absolute_audit.get("summary") or {}
        checks.append(
            {
                "name": f"{window_name}_crypto_absolute_review_audit_present",
                "ok": int(crypto_absolute_audit.get("row_count") or 0) > 0
                and bool(crypto_absolute_summary)
                and bool(crypto_absolute_audit.get("by_time_to_close_bucket")),
                "row_count": crypto_absolute_audit.get("row_count"),
            }
        )
        crypto_gate = next(
            (
                row
                for row in (absolute_gate.get("categories") or [])
                if isinstance(row, dict) and row.get("category") == "crypto"
            ),
            {},
        )
        if bool(crypto_gate.get("review_allowed")):
            direction_delta = float(crypto_gate.get("direction_match_delta_pts") or 0.0)
            checks.append(
                {
                    "name": f"{window_name}_crypto_absolute_review_mae_improves",
                    "ok": float(crypto_gate.get("mae_delta_pts") or 0.0) < 0.0,
                    "mae_delta_pts": crypto_gate.get("mae_delta_pts"),
                }
            )
            checks.append(
                {
                    "name": f"{window_name}_crypto_absolute_review_direction_bounded",
                    "ok": abs(min(direction_delta, 0.0)) <= max_review_direction_regression + 0.0001,
                    "direction_match_delta_pts": crypto_gate.get("direction_match_delta_pts"),
                    "max_direction_regression_pts": max_review_direction_regression,
                }
            )
            checks.append(
                {
                    "name": f"{window_name}_crypto_absolute_review_labeled",
                    "ok": crypto_gate.get("display_tier") == "review"
                    and "direction_not_cleared" in (crypto_gate.get("review_reasons") or []),
                    "display_tier": crypto_gate.get("display_tier"),
                    "review_reasons": crypto_gate.get("review_reasons"),
                }
            )

        for category in PROTECTED_CATEGORIES:
            row = categories.get(category) or {}
            checks.append(
                {
                    "name": f"{window_name}_{category}_no_mae_or_direction_regression",
                    "ok": bool(row) and _ok_no_regression(row),
                    "mae_delta_pts": row.get("mae_delta_pts"),
                    "direction_match_pct": row.get("direction_match_pct"),
                    "pre_calibration_direction_match_pct": row.get("pre_calibration_direction_match_pct"),
                }
            )
            diagnostic = diagnostic_categories.get(category) or {}
            checks.append(
                {
                    "name": f"{window_name}_{category}_diagnostic_no_direction_regression",
                    "ok": bool(diagnostic)
                    and float(diagnostic.get("direction_match_pct") or 0.0)
                    >= float(diagnostic.get("pre_calibration_direction_match_pct") or 0.0),
                    "direction_match_pct": diagnostic.get("direction_match_pct"),
                    "pre_calibration_direction_match_pct": diagnostic.get("pre_calibration_direction_match_pct"),
                }
            )

        for category in ("crypto", "technology"):
            row = categories.get(category) or {}
            checks.append(
                {
                    "name": f"{window_name}_{category}_gate_passes",
                    "ok": bool(row.get("gate_allowed")),
                    "gate_reasons": row.get("gate_reasons"),
                }
            )

        esports = categories.get("video_games_esports") or {}
        checks.append(
            {
                "name": f"{window_name}_esports_review_only",
                "ok": bool(esports) and not bool(esports.get("gate_allowed")),
                "gate_reasons": esports.get("gate_reasons"),
            }
        )

    world_24h = _summary_by_category(windows.get("24h") or {}).get("world_geopolitics") or {}
    checks.append(
        {
            "name": "24h_world_geopolitics_gate_passes",
            "ok": bool(world_24h.get("gate_allowed")),
            "gate_reasons": world_24h.get("gate_reasons"),
        }
    )

    ok = all(bool(check["ok"]) for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
