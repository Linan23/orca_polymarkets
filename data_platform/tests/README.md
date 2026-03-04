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

The smoke validator is intentionally pragmatic:
- it uses the live configured database
- it checks the actual FastAPI app in-process
- it avoids external API calls

Use it before pushing schema, ingestion, or API changes.
