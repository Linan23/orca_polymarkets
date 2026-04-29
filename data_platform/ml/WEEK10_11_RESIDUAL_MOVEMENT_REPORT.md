# Week 10-11 Residual Whale Movement ML Report

## Scope

The Week 10-11 ML work is scoped to Polymarket only. Kalshi is excluded from the whale-tracking ML claim because wallet-level trader identity cannot be tracked with the same confidence, which blocks reliable whale entry, exit, holding-time, and realized-strategy features.

The current model target is market movement over the next 12h and 24h after an observation cutoff. The implemented approach fits a price-only movement baseline first, then tests whether selected whale features explain the remaining movement residual.

## Objective Alignment

| Objective | Current implementation status |
| --- | --- |
| Use trusted whale scores with arbitrary weights to estimate 12h/24h market movement | Implemented through weighted whale-pressure features, `whale_weight_config.json`, residual movement training, and 12h/24h report outputs. |
| Incorporate whale trading frequency | Implemented through trade-count, trade-share, buy/sell-ratio, distinct-user, and active-day whale features. |
| Include whale entry and exit behavior | Implemented through reconstructed entry/exit lots, partial/full exit counts, unmatched sell counts, holding-time features, and realized profit/ROI features. |
| Tune the model so it does not overperform actual data | Implemented through grouped rolling splits, residual-only whale correction, train-fold feature selection, recurring-feature gates, fold diagnostics, and model-family robustness checks. |

## Final Full-Grid Result

Source report:

```text
data_platform/runtime/ml/final_week10_11_residual_model_comparison_polymarket_trade_covered.json
data_platform/runtime/ml/final_week10_11_residual_model_comparison_polymarket_trade_covered.md
```

Dataset:

```text
data_platform/runtime/ml/resolved_market_snapshot_features_backfilled_second.csv
```

Configuration:

```text
Market scope: Polymarket only
Regime: trade_covered
Rows evaluated: 6842
Selector thresholds: 0.01, 0.02, 0.05
Selector feature caps: 8, 16, 24
Models compared: ridge, random_forest, lightgbm_conservative
```

| Model | 12h RMSE delta | 24h RMSE delta | Stable whale features | Passing folds | Read |
| --- | ---: | ---: | ---: | ---: | --- |
| `ridge` | -0.001698 | -0.001840 | 13 total | 5/8 | Default claim model. |
| `random_forest` | -0.001609 | -0.001112 | 10 total | 6/8 | Supporting benchmark. |
| `lightgbm_conservative` | -0.001464 | -0.000778 | 17 total | 7/8 | Supporting benchmark, but weaker average RMSE lift. |

## Recommendation

Use `ridge` as the current Week 10-11 residual whale movement claim model for the larger Polymarket trade-covered dataset.

The claim should be phrased conservatively:

> Whale behavior improves 12h and 24h residual market-movement prediction on the larger Polymarket trade-covered dataset, with Ridge currently the most stable claim model.

This should not be presented as production trading advice. It is a research model result based on resolved Polymarket history and current backfilled coverage.

## Crypto Up/Down Treatment

Keep crypto up/down markets in the full Polymarket source-of-truth run, but call them out as a caveat.

Reason:
- In the full larger dataset, `ridge` is stable across seeds and wins both 12h and 24h.
- When crypto up/down markets are excluded, the winning model changes to `lightgbm_conservative` for 12h and `random_forest` for 24h.
- This means model choice is segment-sensitive, even though whale lift is still present.

Report language should say:

```text
The main result is measured on the full Polymarket trade-covered dataset. Crypto up/down markets are retained in the source-of-truth evaluation, but segment diagnostics show that model choice is sensitive when those markets are excluded.
```

## Robustness Summary

| Robustness case | Default model | 12h pick | 24h pick | Interpretation |
| --- | --- | --- | --- | --- |
| Larger backfill, seed 42 | `ridge` | `ridge` | `ridge` | Primary claim case. |
| Larger backfill, seed 7 | `ridge` | `ridge` | `ridge` | Seed-stable on larger data. |
| Larger backfill, seed 123 | `ridge` | `ridge` | `ridge` | Seed-stable on larger data. |
| Larger backfill, excluding crypto up/down | `lightgbm_conservative` | `lightgbm_conservative` | `random_forest` | Segment sensitivity caveat. |
| First backfill, seed 42 | `lightgbm_conservative` | `random_forest` | `lightgbm_conservative` | Coverage sensitivity caveat. |
| First backfill, excluding crypto up/down | `random_forest` | `random_forest` | `random_forest` | Coverage and segment sensitivity caveat. |

## Week 10-11 Status

This aligns with the Week 10 target because the ML pipeline is implemented against the server-backed Polymarket data export, with model-ready market snapshots and residual movement reports.

This aligns with the Week 11 target because the model now uses trusted whale scores, configurable whale weights, whale trading frequency, whale entry/exit behavior, holding time, and profit features to evaluate whether whale behavior can improve 12h/24h market-movement predictions.

Remaining caveat for client review: the result is feasible and promising on the larger Polymarket trade-covered dataset, but it is sensitive to market segment and data coverage. The client-facing update should present it as a validated research signal, not a finalized production model.
