# syntax=docker/dockerfile:1.6
#
# Standalone single-container image for Bhulan.
#
# Stage 1 builds the Vite bundle under ``web/``.
# Stage 2 installs Poetry dependencies and copies both the Python source tree
# and the built SPA into a slim runtime image. ``bhulan.api.app`` serves the
# /v1 API and mounts the SPA at ``/`` so one container = the whole app.

# -----------------------------------------------------------------------------
# Stage 1 — build the web bundle
# -----------------------------------------------------------------------------
FROM node:20-alpine AS web-builder
WORKDIR /app/web

COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY web/ ./
# Production build reads VITE_BACKEND_URL at build time. Default to same-origin
# ("") so the SPA posts to /v1/... on whatever host is serving the HTML.
ARG VITE_BACKEND_URL=""
ENV VITE_BACKEND_URL=${VITE_BACKEND_URL}
RUN npm run build

# -----------------------------------------------------------------------------
# Stage 2 — Python runtime
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "poetry==${POETRY_VERSION}"

WORKDIR /app

COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --only main --no-root

COPY bhulan ./bhulan

# Pull in the built SPA; app.py mounts /app/web/dist at "/".
COPY --from=web-builder /app/web/dist ./web/dist
ENV WEB_DIST_DIR=/app/web/dist

# Lock CORS down by default — override for multi-origin deployments.
ENV ALLOWED_ORIGINS="*" \
    API_HOST=0.0.0.0 \
    API_PORT=8000

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
    CMD curl -fsS http://127.0.0.1:8000/v1/healthz || exit 1

CMD ["uvicorn", "bhulan.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
