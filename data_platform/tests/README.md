# Tests

`data_platform/tests/` contains backend smoke, data-quality, readiness, and ML guardrail checks.

## Common Checks

```bash
.venv/bin/python data_platform/tests/smoke_validate.py
.venv/bin/python data_platform/tests/data_quality_check.py
.venv/bin/python data_platform/tests/week45_readiness_check.py
.venv/bin/python data_platform/tests/history_partition_check.py
```

## ML Checks

```bash
.venv/bin/python data_platform/tests/ml_report_endpoint_check.py
.venv/bin/python data_platform/tests/market_scope_finance_check.py
.venv/bin/python data_platform/tests/ml_category_validation_check.py
.venv/bin/python data_platform/tests/ml_market_scope_guardrail_check.py
.venv/bin/python data_platform/tests/ml_trend_magnitude_guardrail_check.py
```

## Model/Feature Checks

```bash
.venv/bin/python data_platform/tests/market_ml_dataset_check.py
.venv/bin/python data_platform/tests/market_ml_baseline_check.py
.venv/bin/python data_platform/tests/market_lightgbm_check.py
.venv/bin/python data_platform/tests/market_feature_set_comparison_check.py
```

## CI Baseline

At minimum, run:

```bash
.venv/bin/python -m compileall data_platform scripts
.venv/bin/python scripts/secret_scan.py
.venv/bin/python data_platform/tests/smoke_validate.py
npm --prefix my-app run lint
npm --prefix my-app run build
```

Some checks require a migrated database and current data. Use `--require-data` only when validating an actual populated environment.
