# ML Starter

This directory now contains the first model-ready dataset export and a baseline classifier.

Current scope:
- Polymarket only
- resolved `user x market` rows for the user-profitability starter
- resolved `market x observation_time` rows for the market-outcome starter
- conservative targets built from normalized resolved history

This is intentionally not the final model.
It is the first reproducible ML step that plugs into the normalized PostgreSQL data layer.

## Files

- [`dataset_builder.py`](dataset_builder.py)
  - exports the resolved user/market feature table
- [`baseline_model.py`](baseline_model.py)
  - trains the first baseline classifier
- [`market_dataset_builder.py`](market_dataset_builder.py)
  - exports the point-in-time market snapshot feature table for outcome prediction
- [`market_baseline_model.py`](market_baseline_model.py)
  - trains the grouped time-aware market outcome baseline and LightGBM variant

## Dataset

The exported dataset is built from:
- `analytics.transaction_fact`
- `analytics.market_contract`
- `analytics.market_event`
- `analytics.user_account`

One row represents one resolved Polymarket `user x condition` observation.

Target:
- `label_positive_realized_pnl`

Important constraints:
- Kalshi is excluded from user-level ML until trader identity is stronger.
- Resolved outcomes reuse the same conservative resolver used by whale scoring.
- Rows are excluded when sells exceed captured buys for an outcome.
- Leakage columns are exported for audit and debugging, but should not be used as model features.

## Primary Market-Level Target

The main ML target for the project should now be treated as:
- market-level outcome prediction from pre-close market state

Current market-level export:
- one row = one resolved Polymarket condition side at one observation time
- observation times are fixed hours-before-close horizons
- target = `label_side_wins`
- forward movement targets = `future_price_delta_12h` and `future_price_delta_24h`
- dataset version = `ml_market_snapshot_v9`

Current whale feature semantics:
- whale participation features are historical-as-of-cutoff, not taken from the latest global whale batch
- trusted/whale labels are recomputed from trade and resolved-market history available at each observation time
- scored whale weighted-pressure features apply the configurable weights to the broader whale cohort for better trade-covered coverage, but stay in explicit ablations until they clear the rolling-RMSE gate
- trusted whale weighted-pressure features use `whale_weight_config.json`
- trusted whale weighted-pressure features include raw and normalized variants scaled by notional, liquidity, and trusted-whale counts
- trusted whale entry/exit and holding features are reconstructed from matched buy/sell lots
- recent trusted whale entry/exit pressure is captured over 1h, 6h, 12h, and 24h pre-cutoff windows
- movement models can optionally apply train-fold whale feature selection using target correlation, which keeps price/context features fixed and rejects weak whale columns before each split is evaluated
- residual movement experiments fit price-only movement first, then test whether selected whale features explain the remaining 12h/24h movement residual
- residual movement recommendations now distinguish raw best residual lift from whale-valid lift that keeps recurring selected whale features
- residual reports include segment diagnostics for short crypto up/down markets versus other market durations and families
- residual reports include fold-level RMSE-delta confidence diagnostics as a stability check, not formal statistical proof
- residual analysis supports `--segment short_non_crypto` and `--exclude-family crypto_updown` for scoped validation
- residual analysis supports `--estimator ridge` for a simpler linear residual diagnostic alongside tree models
- historical current exposure is approximated from open shares valued at average buy price
- resolved outcomes prefer Polymarket Gamma `outcomePrices`, with price thresholds only as fallback

Operational note:
- the corrected historical market export is materially slower than the earlier prototype because it recomputes whale state across ordered observation cutoffs

This is closer to the actual research goal than only predicting whether a trader made money.

## Run

Export the model dataset:

```bash
.venv/bin/python data_platform/jobs/export_ml_dataset.py
```

Train the baseline model:

```bash
.venv/bin/python data_platform/jobs/train_ml_baseline.py
```

Export the market-level snapshot dataset:

```bash
.venv/bin/python data_platform/jobs/export_market_ml_dataset.py
```

Export with an alternate whale-weight config:

```bash
.venv/bin/python data_platform/jobs/export_market_ml_dataset.py --whale-weight-config-path data_platform/ml/whale_weight_config.json
```

Train the canonical grouped market model with LightGBM plus rolling diagnostics:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling
```

Train only the trade-covered regime:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling --regime trade_covered
```

Validate closed/matured market-profile predictions, retrain the confidence layer, and generate fresh 12h/24h snapshots:

```bash
.venv/bin/python data_platform/jobs/run_ml_prediction_confidence_cycle.py
```

Train only the closed-market confidence artifact used by market profiles:

```bash
.venv/bin/python data_platform/jobs/train_ml_prediction_confidence.py \
  --output-path data_platform/runtime/ml/market_prediction_confidence_model.json
```

The confidence artifact is trained from `analytics.ml_market_prediction_validation` joined to saved prediction snapshots. The split is chronological, so newer validated markets are held out and the model does not learn from future outcomes when scoring older predictions.

The slower analytics refresh loop now runs the same validation -> confidence training -> snapshot generation sequence when ML validation and snapshots are both enabled.

## Core Equations And Metrics

Client-facing formulas used by the market-profile ML explanation:

- Whale pressure:
  `Whale Pressure = Trade Notional x Whale Trust Score`
  - Trade Notional is the dollar-sized value of the whale trade. Example: buying 2,000 shares at $0.40 creates about $800 of notional.
  - Whale Trust Score is the weight assigned to that wallet based on historical behavior, such as past signal quality, trade size, consistency, and realized performance.
  - Whale Pressure is the weighted size of the whale action. A large trade from a highly trusted whale creates more pressure than the same trade from a lower-trust wallet.
  - This does not mean the market must move. It means the model sees stronger whale evidence for that side of the market.
- Net whale pressure:
  `Net Pressure = Buy Pressure - Sell Pressure`
  - Buy Pressure is the sum of weighted whale buy activity for the market side being forecast.
  - Sell Pressure is the sum of weighted whale sell or exit activity for that same side.
  - Net Pressure compares whether trusted whale activity is mostly entering or leaving.
  - Positive net pressure means whales are leaning into the side. Negative net pressure means whales are reducing or exiting exposure.
  - Example: if buy pressure is 12,000 and sell pressure is 4,000, net pressure is +8,000, which suggests whale activity is more bullish for that side.
- Predicted odds:
  `Predicted Future Odds = Current Odds + Predicted Odds Change`
  - Current Odds is the market's Yes or No probability at the prediction start time. On the dashboard, this is usually the probability at the whale entry time or latest prediction snapshot.
  - Predicted Odds Change is the model's estimated movement over the forecast window, measured in percentage points. A `+4 pts` change means the model expects the probability to rise by 4 percentage points, not multiply by 4%.
  - Predicted Future Odds is the model's expected probability after the selected timeframe, usually 12h or 24h.
  - Example: if Current Odds are 62% and the Predicted Odds Change is +4 pts, then Predicted Future Odds are 66%.
  - Example: if Current Odds are 62% and the Predicted Odds Change is -5 pts, then Predicted Future Odds are 57%.
- Prediction error:
  `Error = Actual Odds - Predicted Odds`
  - Actual Odds is the real market probability observed after the same forecast window. For a 12h forecast, this is the market probability 12 hours after the prediction start time when that data is available.
  - Predicted Odds is the model's forecast for that same future time.
  - Error keeps the sign, so it shows whether the prediction was too low or too high.
  - Positive error means the actual market ended higher than the model predicted, so the model underpredicted.
  - Negative error means the actual market ended lower than the model predicted, so the model overpredicted.
  - Example: if the model predicted 66% and the actual 12h odds were 70%, the error is +4 pts.
- Absolute error:
  `Absolute Error = |Actual Odds - Predicted Odds|`
  - Absolute Error measures how far off the model was, ignoring direction.
  - Smaller absolute error means the predicted probability was closer to the real market probability.
  - This is useful when the team cares about closeness, not whether the model was too high or too low.
  - Example: if the model predicted 66% and actual odds were 70%, the absolute error is 4 pts. If the actual odds were 62%, the absolute error is also 4 pts.
- Direction accuracy:
  `Accuracy = Correct Direction Predictions / Total Validated Predictions`
  - A correct direction means the model predicted the same up/down movement direction that actually happened during the validation window.
  - Total Validated Predictions are predictions that already reached their 12h or 24h validation time and have enough actual market data to compare against.
  - If the model predicts the Yes probability will rise and the actual Yes probability rises, that counts as correct direction.
  - If the model predicts the Yes probability will rise but the actual Yes probability falls, that counts as incorrect direction.
  - This is the main accuracy number shown for validated trend direction, but it does not measure how close the predicted percentage was. Direction accuracy and absolute error should be read together.
- Historical signal reliability:
  `Reliability = Historical Correct Signals / Similar Past Signals`
  - Similar Past Signals are older predictions that look similar to the current one, usually by forecast window, confidence level, signal tier, category, and validation slice when enough data exists.
  - Historical Correct Signals are the similar past predictions that later moved in the predicted direction.
  - Reliability estimates how often this type of signal has worked before.
  - This is used to show Strong, Watch, or Review before the current market has finished validating.
  - Strong means similar signals have historically been more reliable.
  - Watch means the signal has enough support to monitor, but the model is less certain than Strong.
  - Review means the signal does not have enough validation support yet, or it belongs to a weaker/low-data slice.

These equations are used for dashboard explanation and validation reporting. The trained models also use additional engineered features, calibration, and rolling validation checks.

Train the cold-start regime with its dedicated feature path:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling --regime cold_start
```

Train 12h/24h market movement tasks:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task movement_12h --evaluation-mode rolling
.venv/bin/python data_platform/jobs/train_market_model.py --task movement_24h --evaluation-mode rolling
```

Train a movement model with train-fold whale feature selection:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task movement_12h --evaluation-mode rolling --regime trade_covered --feature-selection training_correlation
```

Compare price-only vs whale-informed 12h/24h movement models:

```bash
.venv/bin/python data_platform/jobs/compare_market_movement_feature_sets.py --estimator random_forest
```

Tune 12h/24h movement models and write the Week 10-11 report artifact:

```bash
.venv/bin/python data_platform/jobs/tune_market_movement_models.py
```

Include LightGBM in the movement tuning report:

```bash
.venv/bin/python data_platform/jobs/tune_market_movement_models.py --profiles rf_shallow,rf_regularized,rf_current,lgbm_regularized
```

Try the selected-whale tuning profile:

```bash
.venv/bin/python data_platform/jobs/tune_market_movement_models.py --profiles rf_shallow_selected_whale --regime trade_covered
```

Analyze residual whale movement signal with selector threshold/cap sweeps:

```bash
.venv/bin/python data_platform/jobs/analyze_market_movement_residuals.py --regime trade_covered
```

Run scoped residual diagnostics:

```bash
.venv/bin/python data_platform/jobs/analyze_market_movement_residuals.py --segment short_non_crypto
.venv/bin/python data_platform/jobs/analyze_market_movement_residuals.py --exclude-family crypto_updown --estimator ridge
```

Compare residual model families for the Week 10-11 claim model:

```bash
.venv/bin/python data_platform/jobs/compare_market_movement_residual_models.py --regime trade_covered
.venv/bin/python data_platform/jobs/compare_market_movement_residual_models.py --estimators random_forest,ridge,lightgbm,lightgbm_conservative --regime trade_covered
```

Current Week 10-11 residual movement claim:

On the larger trade-covered backfill, Ridge is the current best residual whale model for both 12h and 24h movement. Random Forest and conservative LightGBM remain supporting benchmarks. The claim is sensitive to data coverage and crypto up/down segmentation, so reports should mention that model choice changes on the smaller backfill and when crypto up/down markets are excluded.

Final tracked Week 10-11 report artifact:
- [`WEEK10_11_RESIDUAL_MOVEMENT_REPORT.md`](WEEK10_11_RESIDUAL_MOVEMENT_REPORT.md)
- [`CLIENT_ML_UPDATE.md`](CLIENT_ML_UPDATE.md)

Backend report metadata endpoint:
- `GET /api/ml/week10-11/residual-movement`

| Robustness case | Default model | 12h pick | 24h pick | Interpretation |
| --- | --- | --- | --- | --- |
| Larger backfill, seed 42 | `ridge` | `ridge` | `ridge` | Primary claim case. |
| Larger backfill, seed 7 | `ridge` | `ridge` | `ridge` | Seed-stable on larger data. |
| Larger backfill, seed 123 | `ridge` | `ridge` | `ridge` | Seed-stable on larger data. |
| Larger backfill, excluding crypto up/down | `lightgbm_conservative` | `lightgbm_conservative` | `random_forest` | Segment sensitivity caveat. |
| First backfill, seed 42 | `lightgbm_conservative` | `random_forest` | `lightgbm_conservative` | Coverage sensitivity caveat. |
| First backfill, excluding crypto up/down | `random_forest` | `random_forest` | `random_forest` | Coverage and segment sensitivity caveat. |

Train the grouped time-aware market baseline:

```bash
.venv/bin/python data_platform/jobs/train_market_ml_baseline.py
```

Train the grouped time-aware LightGBM market model:

```bash
.venv/bin/python data_platform/jobs/train_market_lightgbm.py
```

Compare price-only vs price-plus-whale models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets.py
```

Compare LightGBM price-only vs price-plus-whale models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets_lightgbm.py
```

Compare Random Forest vs LightGBM on the same grouped market split:

```bash
.venv/bin/python data_platform/jobs/compare_market_model_families.py
```

Analyze residual whale signal beyond price:

```bash
.venv/bin/python data_platform/jobs/analyze_market_whale_signal.py
```

Analyze whale lift on the trade-covered regime only:

```bash
.venv/bin/python data_platform/jobs/analyze_market_whale_signal.py --regime trade_covered
```

Analyze whale feature sparsity and movement ablations:

```bash
.venv/bin/python data_platform/jobs/analyze_whale_feature_ablation.py
```

macOS note:
- LightGBM may also require the native OpenMP runtime
- if import/loading fails with `libomp.dylib` missing, run:

```bash
brew install libomp
```

Run the validation check:

```bash
.venv/bin/python data_platform/tests/ml_baseline_check.py --require-data
```

Validate the market-level dataset export:

```bash
.venv/bin/python data_platform/tests/market_ml_dataset_check.py --require-data
```

Validate the market-level baseline:

```bash
.venv/bin/python data_platform/tests/market_ml_baseline_check.py --require-data
```

Validate the LightGBM market path:

```bash
.venv/bin/python data_platform/tests/market_lightgbm_check.py --require-data
```

Validate the LightGBM transition gate:

```bash
.venv/bin/python data_platform/tests/market_lightgbm_transition_check.py --require-data
```

Validate the residual whale-signal analysis:

```bash
.venv/bin/python data_platform/tests/market_whale_signal_check.py --require-data
```

Validate the Week 10-11 movement tuning report:

```bash
.venv/bin/python data_platform/tests/market_movement_tuning_report_check.py --require-data
```

Validate the whale feature ablation report:

```bash
.venv/bin/python data_platform/tests/market_whale_feature_ablation_check.py --require-data
```

Validate the residual whale movement report:

```bash
.venv/bin/python data_platform/tests/market_movement_residual_check.py --require-data
```

Validate the market feature-set comparison:

```bash
.venv/bin/python data_platform/tests/market_feature_set_comparison_check.py --require-data
```

## Outputs

Outputs land under `data_platform/runtime/ml/`:

- `resolved_user_market_features.csv`
- `resolved_user_market_features.metadata.json`
- `profitability_baseline_model.pkl`
- `profitability_baseline_metrics.json`
- `profitability_baseline_feature_importance.csv`
- `resolved_market_snapshot_features.csv`
- `resolved_market_snapshot_features.metadata.json`
- `market_outcome_baseline_model.pkl`
- `market_outcome_baseline_metrics.json`
- `market_outcome_baseline_feature_importance.csv`
- `market_outcome_lightgbm_model.pkl`
- `market_outcome_lightgbm_metrics.json`
- `market_outcome_lightgbm_feature_importance.csv`
- `market_model_training_report.json`
- `market_feature_set_comparison.json`
- `market_feature_set_comparison_lightgbm.json`
- `market_model_family_comparison.json`
- `market_whale_signal_analysis.json`
- `market_movement_feature_set_comparison.json`
- `market_movement_tuning_report.json`
- `market_whale_feature_ablation_report.json`
- `market_movement_residual_report.json`
- `week10_11_market_movement_report.md`
- `week10_11_market_movement_residual_report.md`

## Why This Is The Right First Step

This starter now gives you:
- a reproducible feature export contract
- retained sparse pre-cutoff snapshots with explicit coverage flags instead of dropping no-trade horizons
- static cold-start metadata features from question/title/description/category/tag text plus smoothed priors built only from older resolved markets
- a real target variable tied to resolved market outcomes
- a canonical LightGBM path with rolling diagnostics on exact `market_end_time` buckets
- a benchmark Random Forest reference for transition safety
- regime-aware outcome and whale reports that separate trade-covered rows from cold-start rows
- a residual whale-signal report that measures lift beyond price, broken out by horizon band and by trade-covered vs cold-start regime
- a movement tuning report that compares compact estimator profiles for 12h/24h targets and only accepts whale lift on rolling RMSE
- a whale feature ablation report that separates sparse, recent, timing, weighted-pressure, notional, and behavior feature groups
- a residual whale movement report that tests whether selected whale features explain what price-only movement models miss

The next ML step after this should be feature refinement on the same grouped split and residual whale-signal task, not uncontrolled model sprawl.

## Interpretation Caution

The current baseline uses each trader's full resolved market trajectory.
That makes it useful as:
- a benchmark for whether the exported features are informative
- a foundation for future feature engineering
- a reproducible starting point for the semester ML track

It does not yet mean:
- you have a production market-outcome model
- you have demonstrated whale predictive lift beyond price
- the current metrics should be presented as live deployment performance

Whale claims should come from the residual `whale_signal` report rather than the compatibility market-outcome classifier alone.
Whale lift is now judged on the `trade_covered` regime rather than the mixed export, because cold-start rows behave like a separate neutral problem.
Price saturation is now reported per export and per fold because it can change as the resolved dataset grows.
