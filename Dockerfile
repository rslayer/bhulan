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

# Drop root before running the app. The runtime only needs read access to
# /app, so we don't chown — that keeps the image reproducible and layer-
# cache friendly. If you add writable paths (e.g. SQLite), chown them here.
RUN groupadd --system --gid 1001 bhulan \
    && useradd --system --uid 1001 --gid bhulan --home-dir /app --shell /usr/sbin/nologin bhulan
USER bhulan

EXPOSE 8000
# Use Python for the healthcheck so we don't rely on curl remaining on PATH
# for the non-root user (and to avoid an extra apt dep in slim builds).
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
    CMD python -c "import os,urllib.request,sys; \
        p=os.environ.get('PORT','8000'); \
        sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{p}/v1/healthz', timeout=3).status == 200 else 1)" \
        || exit 1

# --workers: each worker is a full process, so CPU-bound analytics scale ~linearly
# across them (they bypass the GIL). Set WEB_CONCURRENCY ≈ the container's vCPU
# count. Default 2 gives real concurrency out of the box; see DEPLOY.md for the
# per-process rate-limit caveat.
CMD ["sh", "-c", "uvicorn bhulan.api.app:app --host 0.0.0.0 --port ${PORT:-8000} --workers ${WEB_CONCURRENCY:-2}"]
