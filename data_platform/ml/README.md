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
- dataset version = `ml_market_snapshot_v3`

Current whale feature semantics:
- whale participation features are historical-as-of-cutoff, not taken from the latest global whale batch
- trusted/whale labels are recomputed from trade and resolved-market history available at each observation time
- historical current exposure is approximated from open shares valued at average buy price

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
- `market_feature_set_comparison.json`
- `market_feature_set_comparison_lightgbm.json`
- `market_model_family_comparison.json`

## Why This Is The Right First Step

This starter gives you:
- a reproducible feature export contract
- a real target variable tied to resolved market outcomes
- baseline metrics you can compare future models against
- a clean bridge to later LightGBM/XGBoost work

The next ML step after this should be LightGBM/XGBoost experimentation on the same grouped split, then feature refinement, not uncontrolled model sprawl.

## Interpretation Caution

The current baseline uses each trader's full resolved market trajectory.
That makes it useful as:
- a benchmark for whether the exported features are informative
- a foundation for future feature engineering
- a reproducible starting point for the semester ML track

It does not yet mean:
- you have a production market-outcome model
- you have a forward-looking trading signal
- the current metrics should be presented as live deployment performance

The market-level snapshot export is the bridge to that next model.
Its next step should be time-based training and evaluation, not a random split on grouped condition rows.
