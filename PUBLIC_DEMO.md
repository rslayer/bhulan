# Public Demo Mode

Bhulan can run as a free, public, stateless GPS analytics tool. This is the recommended launch mode for an open-source demo.

## What is enabled

- Paste coordinates and compute stops, trips, hotspots, and summary metrics.
- Upload GPX, KML, FIT, CSV, JSON, GeoJSON, or plain lat/lon text.
- Compare multiple tracks side by side.
- Use the API docs at `/docs`.

## What is disabled

- User accounts and saved history: `BHULAN_AUTH_ENABLED=false`.
- Mongo-backed ingestion endpoints unless you configure MongoDB and `API_KEY`.
- Persistent storage of uploaded or pasted GPS data.

## Privacy posture

In public demo mode, Bhulan processes GPS input in memory for the duration of the request and returns the computed report. It does not save pasted coordinates, uploaded files, or generated insights to a database. Operators should still avoid logging request bodies at the proxy or hosting layer.

## Render deployment

1. Push this repository to GitHub.
2. In Render, create a new Blueprint or Web Service from the repo.
3. Use the included `render.yaml`, or choose Docker manually with `Dockerfile`.
4. Confirm these environment variables:

| Variable | Value |
| --- | --- |
| `ALLOWED_ORIGINS` | `*` |
| `BHULAN_AUTH_ENABLED` | `false` |
| `ENABLE_PROMETHEUS` | `true` |
| `RATE_LIMIT_INSIGHTS` | `30/minute` |
| `RATE_LIMIT_PLOT` | `60/minute` |

After deploy, check:

- `/v1/healthz` returns `{"status": "ok"}`.
- `/docs` loads the API docs.
- The home page loads the React app.

## When to add persistence

Add MongoDB only when you want the legacy ingestion subsystem. Add SQLite auth/history only when users explicitly need saved runs. For a free public demo, keep both off.
