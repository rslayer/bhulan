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
| `AUTH_DEV_MODE` | `false` | Keeps magic-link tokens out of HTTP responses. |
| `ENABLE_PROMETHEUS` | `false` | Keeps `/metrics` unavailable on the public demo. |
| `API_KEY` | long random value | Protects legacy `/config`, `/metrics`, and `/jobs/*` endpoints if enabled later. |
| `RATE_LIMIT_INSIGHTS` | `10/minute` | Per-IP slowapi limit for reports and compare. |
| `RATE_LIMIT_PLOT` | `30/minute` | Per-IP slowapi limit for plotting and file parsing. |
| `MAX_PUBLIC_TEXT_BYTES` | `1000000` | Rejects very large pasted text before parsing. |
| `MAX_PUBLIC_POINTS` | `25000` | Per-track public cap before analytics work. |
| `MAX_COMPARE_TOTAL_POINTS` | `50000` | Aggregate cap across all tracks in compare. |
| `MAX_UPLOAD_BYTES` | `5242880` | Public-demo upload cap, 5 MiB. |
| `ENABLE_REVERSE_GEOCODING` | `false` | Keeps outbound per-stop geocoding disabled. |
| `WEB_DIST_DIR` | `/app/web/dist` | Where the Docker image copies the Vite bundle. |

Health checks should use `/v1/healthz`, not `/health/ready`. The latter checks MongoDB and is only relevant for the legacy ingestion subsystem.

## Privacy-safe public demo mode

Use [PUBLIC_DEMO.md](PUBLIC_DEMO.md) as the public operations note. In this mode:

- `/v1/insights`, `/v1/plot/validate`, `/v1/compare`, and `/v1/parse/file` are stateless.
- Uploaded/pasted GPS data is processed in memory for the request and is not persisted by Bhulan.
- `/v1/auth/*` and `/v1/history/*` return unavailable unless `BHULAN_AUTH_ENABLED=true`.
- `/v1/capabilities` tells the frontend whether auth/history should be shown.
- Reverse geocoding is deployment-gated and disabled by default to avoid outbound-call abuse.
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
The included GitHub Actions Fly workflow is manual-only; Render should be
connected through Render's dashboard/blueprint.

## Optional: persistence

Only add persistence when the product needs it:

- **MongoDB**: required for legacy ingestion endpoints (`/ingest/trackpoints`, `/jobs/*`). Set `MONGO_URI` and protect ingestion/admin endpoints with `API_KEY`.
- **SQLite auth/history**: set `BHULAN_AUTH_ENABLED=true`, `BHULAN_DB_PATH` to a writable persistent path, and `BHULAN_AUTH_SECRET` to a long random value. Configure SMTP before inviting real users.

For the first free open-source release, leave both off.

## Scaling: concurrency and throughput

The `/v1` analytics are CPU-bound (NumPy plus Python), so a **single** uvicorn
worker is GIL-bound — measured ~60 req/s and ~425 ms p50 for a 300-point track
under concurrent load. Each `uvicorn --workers N` is a **separate process**, so
CPU-bound work scales ~linearly across workers up to the box's vCPU count.

- **Set `WEB_CONCURRENCY` ≈ the container's vCPU count.** The start command
  (Dockerfile / Procfile) defaults to `--workers ${WEB_CONCURRENCY:-2}`. Each
  worker is a full process (its own memory), so on a **constrained free tier**
  (e.g. Render free ≈ 0.1 CPU / 512 MB) set `WEB_CONCURRENCY=1` — two processes
  there only contend for one core and double the memory footprint. On a 2-vCPU
  box use `2`, on a 4-vCPU box `4` (≈ that many × the single-process throughput).
- Behaviour is correct under concurrency regardless: a load test of 200
  simultaneous identical requests returned byte-identical bodies (no shared-state
  races), no request returned a 5xx under contention, and a burst of 15k-point
  requests stayed bounded (~5 s each) rather than tying up a worker — the stop
  scan is capped (see `spec/adrs/0016-stop-scan-work-budget.md`).

### Rate-limit caveat with multiple workers

The per-IP rate limiter (`slowapi`) uses **in-memory, per-process** storage, so
with `N` workers each keeps its own counters — the effective limit becomes
roughly `N ×` the configured value per IP (e.g. `30/minute` per worker). That is
fine for a demo (it stays a guard, not an exact quota). For an **exact** limit
shared across workers, point slowapi at a shared store — pass a Redis
`storage_uri` to the `Limiter` in `bhulan/api/limiter.py`. A 429 now carries a
`Retry-After` header (plus `X-RateLimit-*`) so a client knows how long to back
off instead of hammering.
