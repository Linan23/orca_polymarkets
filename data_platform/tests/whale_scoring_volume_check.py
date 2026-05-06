"""Validate that whale eligibility is based on betting volume, not profit."""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.services.whale_scoring import (  # noqa: E402
    ResolvedUserPerformance,
    WhaleMetricInput,
    compute_whale_scores,
)


def _metric(
    *,
    user_id: int,
    trade_count: int,
    active_days: int,
    trade_volume: float,
    current_exposure: float = 0.0,
    is_likely_insider: bool = False,
) -> WhaleMetricInput:
    return WhaleMetricInput(
        user_id=user_id,
        platform_id=2,
        platform_name="polymarket",
        external_user_ref=f"wallet-{user_id}",
        is_likely_insider=is_likely_insider,
        sample_trade_count=trade_count,
        distinct_markets=3,
        active_trade_days=active_days,
        total_notional=trade_volume,
        current_exposure=current_exposure,
    )


def main() -> int:
    unresolved_high_volume = _metric(user_id=1, trade_count=12, active_days=3, trade_volume=30_000)
    low_volume = _metric(user_id=2, trade_count=12, active_days=3, trade_volume=4_999)
    not_enough_days = _metric(user_id=3, trade_count=12, active_days=2, trade_volume=30_000)
    insider = _metric(user_id=4, trade_count=12, active_days=3, trade_volume=40_000, is_likely_insider=True)

    scores = compute_whale_scores(
        [unresolved_high_volume, low_volume, not_enough_days, insider],
        resolved_performance_by_user={},
    )
    by_user = {item.metric.user_id: item for item in scores}

    assert by_user[1].is_whale is True
    assert by_user[1].profitability_score == 0.0
    assert by_user[2].is_whale is False
    assert by_user[3].is_whale is False
    assert by_user[4].is_whale is False

    exposure_only_volume = _metric(user_id=5, trade_count=12, active_days=3, trade_volume=1_000, current_exposure=30_000)
    exposure_scores = compute_whale_scores(
        [exposure_only_volume, low_volume],
        resolved_performance_by_user={
            5: ResolvedUserPerformance(
                user_id=5,
                resolved_market_count=0,
                winning_market_count=0,
                realized_pnl=0.0,
                realized_roi=0.0,
                excluded_market_count=0,
            )
        },
    )
    exposure_by_user = {item.metric.user_id: item for item in exposure_scores}
    assert exposure_by_user[5].is_whale is False

    ranked_metrics = [
        _metric(user_id=user_id, trade_count=12, active_days=3, trade_volume=10_000 + (user_id * 1_000))
        for user_id in range(10, 20)
    ]
    ranked_scores = compute_whale_scores(ranked_metrics, resolved_performance_by_user={})
    assert all(item.is_whale for item in ranked_scores)
    assert sum(1 for item in ranked_scores if item.is_top_trust_whale) == 3

    print("whale scoring volume check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
