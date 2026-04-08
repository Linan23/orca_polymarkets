# Production Deployment

This repository now includes a production-oriented Docker stack for hosting the site and PostgreSQL on a VM:

- [`compose.prod.yaml`](../compose.prod.yaml) for PostgreSQL, FastAPI, and the public web proxy
- [`docker/python-api.prod.Dockerfile`](../docker/python-api.prod.Dockerfile) for the packaged API image
- [`docker/web.prod.Dockerfile`](../docker/web.prod.Dockerfile) plus [`docker/Caddyfile`](../docker/Caddyfile) for the built frontend and reverse proxy
- [`scripts/deploy_prod.sh`](../scripts/deploy_prod.sh) for build, DB startup, Alembic migration, and service bring-up
- [`.env.production.example`](../.env.production.example) for the server-side environment template
- [`scripts/manage_app_accounts.py`](../scripts/manage_app_accounts.py) for creating and promoting moderator/admin accounts from the terminal

## Server bootstrap

1. Install Docker Engine and the Docker Compose plugin on the VM.
2. Clone the repository onto the VM.
3. Copy the production env template:

```bash
cp .env.production.example .env.production
```

4. Fill in:

- `SITE_ADDRESS`
- `FRONTEND_ORIGIN`
- `POSTGRES_PASSWORD`
- `DATABASE_URL`
- `SESSION_COOKIE_SECURE=true` once HTTPS is active
- optional ingestion keys such as `DUNE_API_KEY`

Keep `DATABASE_URL`, `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` consistent.

For first bring-up on a raw VM, `SITE_ADDRESS=http://SERVER_IP` and `FRONTEND_ORIGIN=http://SERVER_IP` are acceptable.
After DNS and TLS are ready, switch to the final domain and set `SESSION_COOKIE_SECURE=true`.

## Deploy

Run the deployment helper from the repo root:

```bash
./scripts/deploy_prod.sh
```

That script:

- builds the production API and web images
- starts PostgreSQL
- waits for DB health
- runs `alembic upgrade head`
- starts the API and public web services

Verify the stack:

```bash
curl -s http://127.0.0.1/health
curl -s http://127.0.0.1/api/status/ingestion
docker compose --env-file .env.production -f compose.prod.yaml ps
```

## First privileged account

Create the first admin or moderator from the VM terminal after deployment:

```bash
docker compose --env-file .env.production -f compose.prod.yaml run --rm \
  api python scripts/manage_app_accounts.py create \
  --email admin@example.com \
  --display-name "Site Admin" \
  --role admin \
  --generate-password
```

List accounts:

```bash
docker compose --env-file .env.production -f compose.prod.yaml run --rm \
  api python scripts/manage_app_accounts.py list --include-inactive
```

Promote an existing account:

```bash
docker compose --env-file .env.production -f compose.prod.yaml run --rm \
  api python scripts/manage_app_accounts.py set-role \
  --email moderator@example.com \
  --role moderator
```

## Moderator model

The first RBAC pass adds three account roles:

- `viewer`
- `moderator`
- `admin`

Current behavior:

- normal signups are created as `viewer`
- moderators can access `/api/admin/session` and `/api/admin/accounts`
- admins can update account role or activation state with `PATCH /api/admin/accounts/{account_id}`

This is the foundation for a future moderator UI. It does not replace Git-based code changes or Alembic migrations.

## Operating model

Use this split for future maintenance:

- Code changes: GitHub PR -> pull on VM -> rerun `./scripts/deploy_prod.sh`
- Schema changes: add Alembic migration -> deploy -> migrate
- Content or account moderation: authenticated admin/moderator workflows
- Manual database access: SSH into the VM and use `docker compose exec db psql ...` or a tunnel; do not expose PostgreSQL publicly
