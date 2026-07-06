# bhulan

An open-source GPS data processing and mobility insights platform.

[Bhulan](https://en.wikipedia.org/wiki/Indus_river_dolphin) transforms raw GPS coordinates into actionable mobility insights — stops, trips, hotspots, and summary metrics. Paste coordinates, upload a GPX/KML/FIT file, or send JSON via the API and get back a structured report you can visualise on a map.

## Requirements

* [Python 3.10+](https://www.python.org/downloads/)
* [Node.js 20+](https://nodejs.org/) (for the web frontend)
* [Poetry 1.8+](https://python-poetry.org/docs/#installation)
* MongoDB is **not** required for the public analytics surface (`/v1/insights`, `/v1/plot`, `/v1/compare`). It is only needed for the legacy ingestion endpoints (`/ingest/trackpoints`, `/jobs/*`).

## Public demo

Try Bhulan live: **https://bhulan.onrender.com**

Bhulan is designed to launch as a free, stateless open-source demo: pasted coordinates and uploaded files are processed in memory and are not stored when auth/history is disabled. See [PUBLIC_DEMO.md](PUBLIC_DEMO.md) for the recommended privacy-safe launch mode.

The fastest hosted path is Render using the included `render.yaml` and `Dockerfile`. The single container builds the React app, serves it through FastAPI, and exposes `/v1/healthz` for health checks.

The public instance runs as **anonymous GPS mobility insights; no account required; no saved coordinates**. `BHULAN_AUTH_ENABLED=false` keeps sign-in and history controls hidden.

## Quick start

```bash
# Backend
poetry install
poetry run uvicorn bhulan.api.app:app --reload     # http://localhost:8000

# Frontend (in a second terminal)
cd web
npm install
npm run dev                                         # http://localhost:5173
```

The Vite dev server proxies `/v1` requests to the backend automatically.

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/insights` | Compute stops, trips, hotspots, and summary metrics |
| `POST` | `/v1/plot/validate` | Validate and normalise coordinates for map plotting |
| `POST` | `/v1/compare` | Compare 2+ tracks side by side with shared hotspots |
| `POST` | `/v1/parse/file` | Upload a GPX, KML, or FIT file and get parsed points |
| `GET`  | `/v1/healthz` | Liveness probe |

Interactive API docs are available at `/docs` (Swagger UI) when the server is running.

## Input formats

The `/v1/insights` and `/v1/plot/validate` endpoints accept either:

- **Structured JSON** — an array of `{ lat, lon, ts_utc?, speed_mps? }` objects.
- **Raw text** — CSV (with or without headers), JSON arrays, GeoJSON FeatureCollections, or plain `lat,lon` lines.

File upload (`/v1/parse/file`) supports GPX, KML, and FIT formats.

## Sample data

The web app ships with synthetic sample tracks for first-time visitors:

- `/samples/city-walk.csv` — short walk with a cafe stop.
- `/samples/commute-with-stop.json` — drive with a fuel/rest stop.
- `/samples/delivery-route.geojson` — multi-stop delivery route.
- `/samples/trail-hike.gpx` — out-and-back trail with a viewpoint rest.

These files are served by the React app and linked from the input panel on
the public demo.

## Key concepts

* **Stop** — a stationary period within a configurable radius (default 50 m) lasting at least a configurable duration (default 5 minutes).
* **Trip** — a contiguous sub-track between two extended stops or data gaps.
* **Hotspot** — a region of high sample density detected via grid binning and connected-component clustering.
* **Reverse geocoding** — optional Nominatim-backed place name lookup for stops and hotspots (set `geocode_stops: true`).

## Running tests

```bash
# All backend tests (Mongo integration tests skip when Mongo is unavailable)
poetry run pytest --no-cov -q

# Analytics + API tests only (what CI runs)
poetry run pytest tests/unit/ tests/integration/test_insights_api.py --no-cov -q

# Frontend typecheck + build + tests
cd web && npm run build && npm test
```

## Docker

```bash
docker compose up          # Builds and runs the full app at http://localhost:8000
```

See [DEPLOY.md](DEPLOY.md) for Render, Fly.io, and split frontend/API deployment options.

## Project structure

```
bhulan/
├── analytics/     # Pure stateless GPS algorithms (stops, trips, hotspots, geodesy)
├── api/           # FastAPI routes and middleware
├── auth/          # Magic-link authentication + SQLite history (opt-in)
├── config/        # Pydantic settings
├── ingestion/     # Multi-source data ingestion (files, webhooks, Kafka, MQTT)
├── models/        # Canonical schema + vendor adapters
├── storage/       # MongoDB repository abstraction
└── core/          # Logging utilities
web/               # React + Vite + Leaflet frontend
legacy/            # Original Python 2 processing scripts (archived)
```

## License

[MIT](LICENSE)
