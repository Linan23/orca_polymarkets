# Orca Polymarket Dashboard

Orca is a Polymarket-focused analytics dashboard that tracks whale activity, trust-weighted trader behavior, market coverage, and ML-assisted 12h/24h probability forecasts. The project is built for handoff: code, jobs, configuration, and documentation are organized so a new developer can understand the system without relying on informal notes.

## Scope

- Supported market source: Polymarket only.
- Focus categories: Politics, Geopolitics, Crypto, Technology, Video Game/Esports, and Finance.
- Core users: dashboard viewers, project maintainers, and future developers.
- Secrets, SMTP credentials, VM passwords, and API tokens must stay outside Git.

## Architecture

```text
Polymarket APIs
  -> data_platform/jobs ingest and refresh jobs
  -> PostgreSQL analytics/raw schemas
  -> FastAPI read/auth API
  -> React dashboard

Closed market history
  -> ML validation jobs
  -> prediction snapshots and confidence artifacts
  -> Market Profile ML Trend cards
```

## Start Here

- Local setup: [docs/SETUP_LOCAL.md](docs/SETUP_LOCAL.md)
- VM deployment: [docs/DEPLOY_VM.md](docs/DEPLOY_VM.md)
- Architecture overview: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- API overview: [docs/API_OVERVIEW.md](docs/API_OVERVIEW.md)
- ML system: [docs/ML_SYSTEM.md](docs/ML_SYSTEM.md)
- Operations and maintenance: [docs/OPERATIONS.md](docs/OPERATIONS.md)
- Data retention: [docs/DATA_RETENTION.md](docs/DATA_RETENTION.md)
- Handoff notes: [docs/HANDOFF.md](docs/HANDOFF.md)

Subsystem guides:

- Backend/data platform: [data_platform/README.md](data_platform/README.md)
- Scheduled jobs: [data_platform/jobs/README.md](data_platform/jobs/README.md)
- ML code and reports: [data_platform/ml/README.md](data_platform/ml/README.md)
- Backend tests: [data_platform/tests/README.md](data_platform/tests/README.md)
- Frontend: [my-app/README.md](my-app/README.md)

## Setup Scripts

Use the setup scripts when preparing a fresh checkout. They check required tooling, install dependencies, apply database migrations, and print the exact commands needed to start the app.

Local development:

```bash
scripts/setup_local.sh --empty-db
```

Common local options:

```bash
scripts/setup_local.sh --snapshot /path/to/shared_data_snapshot.sql
scripts/setup_local.sh --reset-db --empty-db
scripts/setup_local.sh --skip-smoke
scripts/setup_local.sh --skip-frontend
```

VM deployment:

```bash
scripts/setup_vm.sh
```

Common VM options:

```bash
scripts/setup_vm.sh --no-service-restart
scripts/setup_vm.sh --skip-smoke
scripts/setup_vm.sh --snapshot /path/to/shared_data_snapshot.sql
```

The VM script expects server-only configuration in `/etc/orca.env`. Do not put real credentials in Git.

Setup values are changed in these places:

| Need to change | File/location |
| --- | --- |
| Local developer environment values | `.env` copied from `.env.example` |
| Local setup script behavior | `scripts/setup_local.sh` |
| VM/server environment values | `/etc/orca.env` copied from `.env.production.example` |
| VM setup/deploy script behavior | `scripts/setup_vm.sh` |
| Docker local services | `compose.yaml` |
| Docker production services | `compose.prod.yaml` |
| Frontend API origin | `VITE_API_BASE_URL` in `.env` or `/etc/orca.env` |
| Backend allowed frontend origin | `FRONTEND_ORIGIN` in `.env` or `/etc/orca.env` |
| Auth, signup domains, SMTP | `.env` locally; `/etc/orca.env` on VM |

## Quick Local Run

```bash
scripts/setup_local.sh --empty-db

source .venv/bin/activate
uvicorn data_platform.api.server:app --reload --host 0.0.0.0 --port 8001

npm --prefix my-app run dev -- --host 0.0.0.0 --port 5173
```

Open `http://localhost:5173`.

## Common Checks

```bash
.venv/bin/python -m compileall data_platform scripts
.venv/bin/python scripts/secret_scan.py
.venv/bin/python data_platform/tests/smoke_validate.py
npm --prefix my-app run lint
npm --prefix my-app run build
```

## Repository Rules

- Do not commit `.env`, `/etc/orca.env`, SMTP passwords, app passwords, auth secrets, VM passwords, snapshots, runtime JSONL, or local database exports.
- Keep generated SQL snapshots outside Git and share them through approved release/storage channels.
- Keep new ingest and ML jobs under `data_platform/jobs/`.
- Keep user-facing dashboard changes in `my-app/`.
