# ML

`data_platform/ml/` contains model helpers, weighting logic, datasets, and current report notes for Orca's whale-driven forecast system.

## Purpose

The ML layer supports Market Profile forecasts. It estimates how Polymarket probability may move 12h and 24h after whale activity, then validates those forecasts against actual market movement when data matures.

## Main Concepts

- Whale pressure: trade size multiplied by whale trust score.
- Net support: buy pressure minus sell pressure.
- Forecast probability: current probability plus predicted probability change.
- Confidence: learned from similar validated historical forecasts.
- Validation: comparing predicted direction and magnitude to actual 12h/24h movement.

## Important Files

- `dataset_builder.py`: dataset construction for whale and market features.
- `market_dataset_builder.py`: market-level ML dataset helpers.
- `baseline_model.py`: baseline model helpers.
- `market_baseline_model.py`: market movement model helpers.
- `whale_weighting.py`: trust-weighted whale feature logic.
- `whale_weight_config.json`: configurable whale-weight inputs.

## Report Jobs

```bash
python data_platform/jobs/evaluate_ml_category_validation.py
python data_platform/jobs/evaluate_ml_trend_direction_classifier.py
python data_platform/jobs/evaluate_ml_trend_similarity.py
python data_platform/jobs/evaluate_ml_whale_anchored_delta.py
python data_platform/jobs/generate_ml_market_prediction_snapshots.py
```

## Handoff Notes

- Keep Polymarket as the only market source.
- Keep sports excluded; video games/esports are allowed as a review-oriented category.
- Finance is traditional finance only and remains separate from crypto.
- Treat forecasts as confidence-filtered dashboard signals, not guaranteed results.
- Keep generated report outputs out of Git unless explicitly needed for a handoff artifact.
