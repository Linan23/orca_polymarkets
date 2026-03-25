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
