# Contributing to Bhulan

Thanks for considering a contribution! This guide covers the development
workflow, project conventions, and how to get a pull request merged.

## Getting started

```bash
# Clone and set up the backend
git clone https://github.com/rslayer/bhulan.git
cd bhulan
poetry install          # Python 3.10+ required

# Set up the frontend
cd web
npm install             # Node.js 20+ required
cd ..
```

## Running the app locally

```bash
# Backend (terminal 1)
poetry run uvicorn bhulan.api.app:app --reload   # http://localhost:8000

# Frontend (terminal 2)
cd web && npm run dev                             # http://localhost:5173
```

The Vite dev server proxies `/v1/*` requests to the backend. Open
`http://localhost:5173` to use the full app.

## Running tests

```bash
# Backend — all unit + integration tests
poetry run pytest tests/unit/ tests/integration/test_insights_api.py --no-cov -q

# Frontend — Vitest suite
cd web && npm test

# Quick lint check before pushing
poetry run ruff check bhulan/ scripts/ tests/
cd web && npx tsc -b --noEmit
```

CI runs on every pull request and must be green before merging.

## Code style

| Area | Tool | Config |
|------|------|--------|
| Python lint | [Ruff](https://docs.astral.sh/ruff/) | `pyproject.toml` |
| Python format | Ruff (formatter) | same |
| TypeScript | `tsc --strict` | `web/tsconfig.app.json` |
| Frontend tests | [Vitest](https://vitest.dev/) + [React Testing Library](https://testing-library.com/docs/react-testing-library/intro) | `web/vitest.config.ts` |

- Follow existing patterns: look at neighbouring files before adding something new.
- Keep analytics modules **pure** — no I/O, no network calls.
- Prefer small, focused PRs over large refactors.

## Project layout

```
bhulan/
  analytics/     Pure GPS algorithms (stops, trips, hotspots, geodesy)
  api/           FastAPI routes and middleware
  auth/          Magic-link auth + SQLite history (opt-in)
  config/        Pydantic settings
  ingestion/     Multi-source data ingestion (files, webhooks)
  models/        Canonical TrackPoint schema + vendor adapters
  storage/       MongoDB repository abstraction
  core/          Logging utilities
web/             React + Vite + Leaflet frontend
tests/
  unit/          Backend unit tests
  integration/   API-level integration tests
  system/        End-to-end system tests (legacy modules)
legacy/          Archived Python 2 scripts
```

## Pull request guidelines

1. Branch from `master`: `git checkout -b your-feature master`
2. Keep commits small and well-described.
3. Add tests for new behaviour where practical.
4. Make sure CI passes (`pytest`, `ruff`, `vitest`, TypeScript build).
5. Fill in the PR template — reviewers will look at it first.

## Reporting issues

Open an issue on GitHub with:
- Steps to reproduce
- Expected vs actual behaviour
- Sample input data (if applicable — strip any private GPS data)
