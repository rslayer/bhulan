# Deploying Bhulan

The public `/v1` surface runs anywhere Python 3.10+ can host a FastAPI
app. For a self-hosted production deploy we use:

- **Backend**: Fly.io, via the bundled `fly.toml` + the stage-2 runtime
  section of `Dockerfile`.
- **Frontend**: static hosting (any CDN). The Vite bundle is fully
  client-side; point it at the backend origin via `VITE_BACKEND_URL`
  at build time.

Both halves are stateless. MongoDB is only required for the legacy
`/ingest/trackpoints` + `/jobs/*` endpoints; skip it for the public
insights app.

## Backend — Fly.io

```bash
# One-time: provision the app from the repo root.
flyctl launch --copy-config --dockerfile Dockerfile --no-deploy

# Deploys going forward:
flyctl deploy
```

`fly.toml` declares:

- `internal_port = 8080` — matches `uvicorn --port 8080` in the
  Procfile.
- `processes.app = "uvicorn bhulan.api.app:app ..."` — overrides Fly's
  default FastAPI launcher so we don't depend on the `fastapi` CLI
  binary. The `fastapi[standard]` extra is pinned anyway so either
  launcher works.
- `auto_stop_machines = "stop"` + `min_machines_running = 0` — machines
  suspend on idle and cold-start on the first request (~3 s). Drop
  `auto_stop` to a warm fleet if you need sub-second tail latency.

Env vars worth setting on `flyctl secrets set ...`:

| Var                  | Default      | Notes                                          |
| -------------------- | ------------ | ---------------------------------------------- |
| `ALLOWED_ORIGINS`    | `*`          | Comma-separated. Wildcard drops credentials.   |
| `RATE_LIMIT_INSIGHTS`| `30/minute`  | slowapi syntax.                                |
| `RATE_LIMIT_PLOT`    | `60/minute`  | slowapi syntax.                                |
| `API_KEY`            | (unset)      | Only gates the legacy `/ingest/*` endpoints.   |

## Frontend — static CDN

```bash
# Build with the public backend URL baked in.
cd web
VITE_BACKEND_URL=https://<your-fly-app>.fly.dev npm run build

# Upload web/dist to the CDN of your choice (Netlify, CF Pages, S3+CF,
# Vercel static, GitHub Pages). No build step is needed on the host.
```

Because `fetch` calls in the SPA are prefixed with `VITE_BACKEND_URL`,
a build per target origin is the cleanest option. For single-origin
deploys (SPA mounted at `/` on the backend), build with
`VITE_BACKEND_URL=""` — this is what `docker-compose.yml` and the
`Dockerfile` do.

## CORS

`bhulan/api/app.py` drops `allow_credentials` automatically when
`ALLOWED_ORIGINS` is the wildcard `*`, so the default config works for
cross-origin static-frontend deploys. When you lock down origins to a
specific list, credentials come back on — use this for any future auth
work that relies on cookies or `Authorization` headers with
`withCredentials`.

## Live deploy

The current session brought up:

- Backend: `https://bhulan-oivuldkc.fly.dev/` (routes: `/v1/healthz`,
  `/v1/insights`, `/v1/plot/validate`, `/v1/compare`, `/v1/parse/file`).
- Frontend: a Devin static-hosting URL that posts to the above.

These are throwaway deploys tied to this session's infra. Re-run
`flyctl deploy` from your own Fly account to own the app.
