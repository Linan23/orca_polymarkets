# API Overview

The backend API is a FastAPI application in `data_platform/api/server.py`.

## Inspecting The API

When the API is running locally:

```text
http://localhost:8001/docs
http://localhost:8001/openapi.json
```

On the VM, use the configured API origin from `/etc/orca.env`.

## Endpoint Groups

Homepage/dashboard:

- `GET /api/dashboard/home`
- `GET /api/home/summary`
- `GET /api/analytics/research`
- `GET /api/analytics/market-whale-concentration`

Market and trader profiles:

- `GET /api/markets/{market_slug}/profile/full`
- `GET /api/markets/{market_slug}/ml-trend`
- `GET /api/users/{user_id}/profile/full`

Leaderboards and summaries:

- `GET /api/leaderboard/markets`
- `GET /api/leaderboard/whales`
- `GET /api/analytics/category-summary`

Auth/session:

- `POST /api/auth/signup`
- `POST /api/auth/verify-email`
- `POST /api/auth/login`
- `GET /api/auth/me`
- `POST /api/auth/logout`
- `POST /api/auth/password-reset/request`
- `POST /api/auth/password-reset/confirm`
- `GET /api/auth/csrf`

## API Conventions

- Public dashboard reads should be safe for unauthenticated users.
- Mutating cookie-authenticated routes should use CSRF protection.
- Read endpoints may return cached or snapshot-backed data.
- Optional fields should be treated as optional by the frontend for backward compatibility.

## Adding Endpoints

1. Add SQL/query/service logic under `data_platform/services/`.
2. Add the FastAPI route in `data_platform/api/server.py`.
3. Add or update frontend types in `my-app/src/lib/api.ts`.
4. Add a small smoke or endpoint check under `data_platform/tests/`.
