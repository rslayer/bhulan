# Deploying Bhulan

The recommended public launch is a stateless, single-container web app: FastAPI serves the `/v1` API and the built React app from `web/dist`. MongoDB and auth/history stay disabled, so the demo can run as a free open-source tool without storing user GPS data.

## Recommended: Render free web service

This repo includes a `render.yaml` blueprint and a Dockerfile that works with Render's `PORT` environment variable.

1. Push the repo to GitHub.
2. In Render, create a new Blueprint from the repo, or create a Web Service and choose Docker.
3. Use the included `render.yaml` values.
4. Deploy and verify `/v1/healthz`, `/docs`, and `/`.

Recommended public-demo environment variables:

| Var | Value | Notes |
| --- | --- | --- |
| `ALLOWED_ORIGINS` | `*` | Good for the single-container demo and public API. |
| `BHULAN_AUTH_ENABLED` | `false` | Keeps accounts/history off. |
| `ENABLE_PROMETHEUS` | `true` | Exposes simple process metrics at `/metrics`. |
| `RATE_LIMIT_INSIGHTS` | `30/minute` | Per-IP slowapi limit. |
| `RATE_LIMIT_PLOT` | `60/minute` | Per-IP slowapi limit. |
| `WEB_DIST_DIR` | `/app/web/dist` | Where the Docker image copies the Vite bundle. |

Health checks should use `/v1/healthz`, not `/health/ready`. The latter checks MongoDB and is only relevant for the legacy ingestion subsystem.

## Privacy-safe public demo mode

Use [PUBLIC_DEMO.md](PUBLIC_DEMO.md) as the public operations note. In this mode:

- `/v1/insights`, `/v1/plot/validate`, `/v1/compare`, and `/v1/parse/file` are stateless.
- Uploaded/pasted GPS data is processed in memory for the request and is not persisted by Bhulan.
- `/v1/auth/*` and `/v1/history/*` return unavailable unless `BHULAN_AUTH_ENABLED=true`.
- Mongo-backed `/ingest/trackpoints` and `/jobs/*` are only useful when MongoDB is configured.

Avoid enabling request-body logging at your host or reverse proxy if you run a public demo.

## Optional: split frontend and API

For higher static traffic, host the frontend separately and keep FastAPI as an API service.

```bash
cd web
VITE_BACKEND_URL=https://<your-api-host> npm run build
```

Upload `web/dist` to Cloudflare Pages, Netlify, Vercel static hosting, S3+CloudFront, or GitHub Pages. Set `ALLOWED_ORIGINS` on the API to the exact frontend origin when you move away from the wildcard default.

## Optional: Fly.io

Fly.io is still supported by `fly.toml`:

```bash
flyctl launch --copy-config --dockerfile Dockerfile --no-deploy
flyctl deploy
```

`fly.toml` runs `uvicorn bhulan.api.app:app --host 0.0.0.0 --port 8080` and can mount `/data` for opt-in SQLite auth/history. Use Fly if you prefer its platform or already have billing set up.

## Optional: persistence

Only add persistence when the product needs it:

- **MongoDB**: required for legacy ingestion endpoints (`/ingest/trackpoints`, `/jobs/*`). Set `MONGO_URI` and protect ingestion/admin endpoints with `API_KEY`.
- **SQLite auth/history**: set `BHULAN_AUTH_ENABLED=true`, `BHULAN_DB_PATH` to a writable persistent path, and `BHULAN_AUTH_SECRET` to a long random value. Configure SMTP before inviting real users.

For the first free open-source release, leave both off.
