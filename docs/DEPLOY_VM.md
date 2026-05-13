# VM Deployment

The VM deployment should be updated through Git only. Do not edit application code directly on the server.

## Server Environment

Production-only values live in `/etc/orca.env`. Create it from `.env.production.example` and fill secrets on the VM.

Required values:

```env
DATABASE_URL=
FRONTEND_ORIGIN=
VITE_API_BASE_URL=
AUTH_SECRET_KEY=
ALLOWED_SIGNUP_EMAIL_DOMAINS=
SMTP_HOST=
SMTP_PORT=
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_FROM_EMAIL=
SESSION_COOKIE_SECURE=false
SESSION_COOKIE_SAMESITE=lax
```

Use `SESSION_COOKIE_SECURE=true` only when the site is served through HTTPS.

## Deploy From Git

```bash
cd /home/lynchej/orca_polymarkets
git pull origin main
scripts/setup_vm.sh
```

Useful options:

```bash
scripts/setup_vm.sh --skip-smoke
scripts/setup_vm.sh --no-service-restart
scripts/setup_vm.sh --snapshot /path/to/shared_data_snapshot.sql
```

The script installs backend/frontend dependencies, applies migrations, builds the frontend, restarts services, and checks health endpoints.

## Services

Expected systemd services:

```bash
sudo systemctl status orca-api.service
sudo systemctl status orca-frontend.service
sudo systemctl status orca-ingest-live.service
sudo systemctl status orca-analytics-refresh.service
```

Restart after a deploy:

```bash
sudo systemctl restart orca-api.service
sudo systemctl restart orca-frontend.service
```

## Health Checks

```bash
curl -fsS http://localhost:8001/health
curl -fsS "$FRONTEND_ORIGIN"
```

Then verify in the browser:

- Home page
- Leaderboard
- Following
- Market profile
- Trader profile
- Login/signup/reset flows
- Definitions page

## Deployment Rules

- Pull code from Git; do not patch server files by hand.
- Keep `/etc/orca.env` off Git.
- Keep database snapshots outside Git.
- Rotate SMTP and auth secrets if they are ever exposed.
