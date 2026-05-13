# Frontend

`my-app/` is the React/Vite dashboard.

## Run Locally

```bash
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```

The backend should run on `http://localhost:8001` unless `VITE_API_BASE_URL` is set differently.

## Build And Lint

```bash
npm run lint
npm run build
```

## Structure

- `src/pages/`: route pages such as Home, Leaderboard, Market Profile, User Profile, Following, Login, Definitions, and About.
- `src/homepage/`: homepage summary cards, market coverage, leaderboards, and news/timeline components.
- `src/profile/`: trader profile analytics charts.
- `src/lib/api.ts`: shared API client and response types.
- `src/auth/`: auth context and protected route helpers.
- `src/assets/`: static assets.

## Frontend Rules

- Keep API types backward compatible when backend fields are optional.
- Use shared API helpers instead of ad hoc fetch logic when possible.
- Keep loading behavior cache-friendly and avoid page-level blocking spinners when stale data is available.
- Keep user-facing ML wording simple: forecasts are probability estimates based on whale activity and validation history.
