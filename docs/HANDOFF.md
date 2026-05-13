# Orca Handoff Documentation

## Project Summary

Orca is a Polymarket-only dashboard for whale tracking, market analytics, and ML-assisted probability forecasting. The system helps users see where trusted whales are active, how market probability may move over the next 12 and 24 hours, and how forecasts have performed against actual market movement.

## What Is Included

- React dashboard for home, leaderboard, following, market profiles, trader profiles, definitions, login, and about pages.
- FastAPI backend for dashboard reads, auth, and profile endpoints.
- PostgreSQL data platform with raw and analytics schemas.
- Polymarket ingest and analytics jobs.
- Whale scoring and trust-score logic.
- ML forecast snapshots and validation reports.
- Local and VM setup scripts.
- Professional developer documentation under `docs/`.

## What Is Not Included

- Support for non-Polymarket market sources.
- Public-domain DNS/HTTPS setup by default.
- Trading automation.
- Committed credentials or database snapshots.

## How To Run Locally

Use:

```bash
scripts/setup_local.sh --empty-db
```

Then start:

```bash
source .venv/bin/activate
uvicorn data_platform.api.server:app --reload --host 0.0.0.0 --port 8001
npm --prefix my-app run dev -- --host 0.0.0.0 --port 5173
```

Full details: [SETUP_LOCAL.md](SETUP_LOCAL.md).

## How To Run On The VM

Update through Git:

```bash
cd /home/lynchej/orca_polymarkets
git pull origin main
scripts/setup_vm.sh
```

Do not place VM passwords, SMTP app passwords, or auth secrets in the repository. Put server-only values in `/etc/orca.env`.

Full details: [DEPLOY_VM.md](DEPLOY_VM.md).

## Important Files

- `data_platform/api/server.py`: FastAPI application.
- `data_platform/models/entities.py`: SQLAlchemy models.
- `data_platform/jobs/`: scheduled and manual jobs.
- `data_platform/services/`: shared backend logic.
- `data_platform/ml/`: ML dataset/model helpers and report documentation.
- `my-app/src/pages/`: route-level frontend pages.
- `my-app/src/lib/api.ts`: frontend API client and types.
- `scripts/setup_local.sh`: local developer setup.
- `scripts/setup_vm.sh`: VM deployment setup.

## Handoff Notes

- Keep code changes on `main` unless a new branch process is introduced.
- Run backend and frontend checks before pushing.
- Use Git pull/deploy for the VM; do not hot-patch production files.
- Treat ML forecasts as validated dashboard signals, not guaranteed predictions.
- Keep generated snapshots and runtime artifacts out of Git.
