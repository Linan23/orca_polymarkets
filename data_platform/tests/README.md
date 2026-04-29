# Smoke Validation

This directory contains lightweight operational validation for the local data platform.

Primary entrypoint:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data
```

Optional deeper check with a live dashboard rebuild:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data --build-dashboard
```

Historical lifecycle validation:

```bash
.venv/bin/python data_platform/tests/history_partition_check.py
```

That lifecycle check verifies:
- history tables exist and stay aligned with current-state tables
- partition-shadow tables exist and are populated
- compatibility views return the same counts as the current legacy tables
- partition children exist for the shadow parents

`smoke_validate.py` now includes that lifecycle validation as part of the full smoke run.

Week 4/5 readiness gate (strict mode):

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data
```

Week 4/5 readiness gate (structural mode, CI-safe):

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py
```

Week 6 whale analytics validation:

```bash
.venv/bin/python data_platform/tests/week6_whale_check.py --build --require-data
```

That Week 6 validator now checks:
- positive whale and trusted-whale output on the sample database
- trusted leaderboard row presence and shape
- homepage summary API contract and count consistency
- homepage trusted-whale and market summary object shape

The smoke validator is intentionally pragmatic:
- it uses the live configured database
- it checks the actual FastAPI app in-process
- it avoids external API calls

Use it before pushing schema, ingestion, or API changes.

ML starter validation:

```bash
.venv/bin/python data_platform/tests/ml_baseline_check.py --require-data
```

That ML check verifies:
- the resolved user/market dataset export is non-empty
- the target has both classes
- the baseline trainer produces metrics
- the baseline model does not underperform the majority-class dummy baseline

Market-level ML dataset validation:

```bash
.venv/bin/python data_platform/tests/market_ml_dataset_check.py --require-data
```

That market-level check verifies:
- the market snapshot export is non-empty
- both outcome classes are present
- multiple pre-close horizons are represented
- sparse-snapshot coverage columns are present
- cold-start metadata columns now cover question/title/description/category/tag signals plus smoothed prior columns
- derived residual whale-signal columns are present and bounded

Market-level baseline validation:

```bash
.venv/bin/python data_platform/tests/market_ml_baseline_check.py --require-data
```

That baseline check verifies:
- the market snapshot export is non-empty
- the grouped time split is present
- the model produces metrics
- the model does not underperform the majority-class dummy baseline

LightGBM market-path validation:

```bash
.venv/bin/python data_platform/tests/market_lightgbm_check.py --require-data
```

That LightGBM check verifies:
- the current market dataset exists
- the LightGBM trainer produces metrics
- the LightGBM model does not underperform the majority-class dummy baseline
- the Random Forest vs LightGBM comparison runs on the same grouped split
- rolling ROC-AUC and log-loss diagnostics are present for the transition gate

LightGBM transition validation:

```bash
.venv/bin/python data_platform/tests/market_lightgbm_transition_check.py --require-data
```

That transition check verifies:
- grouped train end-time buckets stay older than grouped test end-time buckets
- LightGBM is declared as the primary model family
- Random Forest remains benchmark-only
- rolling transition-gate metrics are present
- regime-aware transition slices are present for trade-covered and cold-start rows
- the cold-start regime uses the dedicated `cold_start` feature path with broader metadata and smoothed historical priors
- the whale-signal report runs and states whether whale lift is demonstrated in the trade-covered regime

Market feature-set comparison validation:

```bash
.venv/bin/python data_platform/tests/market_feature_set_comparison_check.py --require-data
```

That comparison check verifies:
- the market snapshot export is non-empty
- both comparison models produce metrics
- the comparison uses a grouped time split
- the comparison is running against the historical-as-of-cutoff market export, not the earlier latest-batch whale approximation

LightGBM market feature-set comparison validation:

```bash
.venv/bin/python data_platform/tests/market_feature_set_comparison_lightgbm_check.py --require-data
```

That LightGBM comparison check verifies:
- the current market dataset exists
- both LightGBM comparison models produce metrics
- the comparison uses a grouped time split
- split diagnostics include price saturation and rolling metrics

Residual whale-signal validation:

```bash
.venv/bin/python data_platform/tests/market_whale_signal_check.py --require-data
```

That whale-signal check verifies:
- the residual whale-signal report runs end-to-end
- fixed feature sets are present in the report
- rolling metrics are emitted for the residual models
- sparse-row coverage segments are reported explicitly
- horizon-banded whale analysis is present
- regime-aware whale analysis is present for trade-covered and cold-start rows
- price saturation is reported explicitly
- whale lift is gated on the trade-covered regime
- when whale lift is not demonstrated, the interpretation says so explicitly

Residual movement model-family comparison validation:

```bash
.venv/bin/python data_platform/tests/market_movement_residual_model_comparison_check.py --require-data
```

That comparison check verifies:
- the residual model-family report and markdown are written
- Random Forest and Ridge residual families are compared on the same 12h/24h windows
- the report includes a default estimator recommendation and ranking criteria

Week 10-11 ML report endpoint validation:

```bash
.venv/bin/python data_platform/tests/ml_report_endpoint_check.py
```

That endpoint check verifies:
- the backend-only ML report endpoint is available
- the report is scoped to Polymarket
- the current selected model is Ridge
- the tracked Week 10-11 report and client update markdown are included
