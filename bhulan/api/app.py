"""
FastAPI application for GPS data ingestion.

Provides REST API endpoints for webhook ingestion, job status, and health checks.
"""

import os
import uuid
from typing import Any, Dict, List, Optional, Union

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from bhulan.api.routes.auth import router as auth_router
from bhulan.api.routes.compare import router as compare_router
from bhulan.api.routes.history import router as history_router
from bhulan.api.routes.insights import router as insights_router
from bhulan.auth import db as auth_db
from bhulan.config.settings import settings
from bhulan.ingestion.normalize import normalize_batch
from bhulan.models.canonical import NormalizationResult
from bhulan.models.vendor.generic import create_generic_mapping
from bhulan.models.vendor.geotab import create_geotab_mapping
from bhulan.models.vendor.samsara import create_samsara_mapping
from bhulan.storage.mongo_repo import MongoJobRegistry, MongoTrackPointRepository

app = FastAPI(
    title="Bhulan GPS API",
    description="Mobility insights and map plotting for GPS coordinates, "
                "plus ingestion endpoints for vendor feeds.",
    version="2.1.0"
)


def _parse_origins(raw: str) -> List[str]:
    """Parse a comma-separated CORS allowlist; '*' stays as a single wildcard."""
    raw = (raw or "").strip()
    if raw == "*" or raw == "":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


_cors_origins = _parse_origins(settings.ALLOWED_ORIGINS)
# Browsers reject the ``Access-Control-Allow-Origin: *`` +
# ``Access-Control-Allow-Credentials: true`` combination, so auto-drop
# credentials when origins is wildcard. The public ``/v1`` surface is
# cookie-less anyway — credentials only matter for future auth additions,
# which will ship a narrow origin list.
_cors_allow_credentials = _cors_origins != ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Per-IP rate limiting for the public /v1 surface. Individual limits are wired
# on the route handlers via the ``limiter.limit(...)`` decorator so abusers hit
# a 429 instead of melting the box. slowapi attaches itself through app.state.
limiter = Limiter(key_func=get_remote_address, default_limits=[])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.include_router(insights_router)
app.include_router(compare_router)
app.include_router(auth_router)
app.include_router(history_router)

# Best-effort DB init at startup. The routes also call init_db() lazily so
# tests can point at a tempfile without reimporting this module, but when
# the feature is enabled on a real deploy we want the file + schema to
# exist before the first request lands.
if settings.BHULAN_AUTH_ENABLED:
    try:
        auth_db.init_db(settings.BHULAN_DB_PATH)
    except Exception as exc:  # pragma: no cover - depends on FS perms
        import logging

        logging.getLogger(__name__).warning(
            "Failed to initialize auth DB at %s: %s",
            settings.BHULAN_DB_PATH,
            exc,
        )


@app.get("/v1/healthz", tags=["insights"])
async def healthz() -> Dict[str, str]:
    """
    Liveness probe that doesn't touch Mongo.

    Used by Docker / uptime monitors to decide whether the process is up —
    ``/health/ready`` still exists for the ingestion subsystem's DB check.
    """
    return {"status": "ok"}


def _maybe_repo() -> Optional["MongoTrackPointRepository"]:
    """
    Build a Mongo repo lazily.

    The insights endpoints don't need Mongo, and we don't want the service to
    fail to start when Mongo isn't available. Ingestion endpoints handle a
    missing repo by returning 503.
    """
    try:
        return MongoTrackPointRepository()
    except Exception:
        return None


def _maybe_jobs() -> Optional["MongoJobRegistry"]:
    try:
        return MongoJobRegistry()
    except Exception:
        return None


track_repo = _maybe_repo()
job_registry = _maybe_jobs()


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    """Verify API key if configured."""
    if settings.API_KEY and x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/health/ready")
async def health_check():
    """
    Health check endpoint.

    Returns 200 if service is ready and database is accessible.
    """
    try:
        track_repo.collection.database.client.server_info()
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {str(e)}")


@app.get("/config")
async def get_config():
    """
    Get non-sensitive configuration.

    Returns selected configuration values for debugging.
    """
    return {
        "mongo_db_name": settings.MONGO_DB_NAME,
        "max_batch_size": settings.MAX_BATCH_SIZE,
        "enable_kafka": settings.ENABLE_KAFKA,
        "enable_mqtt": settings.ENABLE_MQTT,
        "enable_prometheus": settings.ENABLE_PROMETHEUS,
    }


@app.post("/ingest/trackpoints", response_model=NormalizationResult)
async def ingest_trackpoints(
    payload: Union[List[Dict[str, Any]], Dict[str, Any]] = Body(...),
    vendor: str = Query("generic", description="Vendor/source identifier"),
    ingest_id: Optional[str] = Query(None, description="Ingestion job ID"),
    x_bhulan_mapping: Optional[str] = Header(None, description="Custom mapping JSON"),
    _: None = Depends(verify_api_key)
):
    """
    Ingest GPS track points via webhook.

    Accepts JSON payload with GPS data, normalizes it, and persists to database.

    Args:
        payload: JSON object or array of GPS records
        vendor: Vendor identifier (generic, geotab, samsara)
        ingest_id: Optional ingestion job ID (generated if not provided)
        x_bhulan_mapping: Optional custom mapping JSON in header

    Returns:
        NormalizationResult with accepted/rejected counts and errors
    """
    if ingest_id is None:
        ingest_id = str(uuid.uuid4())

    if isinstance(payload, dict):
        records = [payload]
    else:
        records = payload

    job_registry.create_job(
        ingest_id=ingest_id,
        source='webhook',
        params={'vendor': vendor, 'record_count': len(records)}
    )

    try:
        if vendor == 'geotab':
            mapping = create_geotab_mapping()
        elif vendor == 'samsara':
            mapping = create_samsara_mapping()
        else:
            mapping = create_generic_mapping()


        result, points = normalize_batch(records, mapping, ingest_id)

        if points:
            track_repo.upsert_batch(points)

        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='succeeded' if result.rejected == 0 else 'partial',
            stats={
                'read': len(records),
                'accepted': result.accepted,
                'rejected': result.rejected
            },
            error_sample=dict(list(result.errors.items())[:10])
        )

        return result

    except Exception as e:
        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='failed',
            error_sample={0: str(e)}
        )
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@app.get("/jobs/{ingest_id}")
async def get_job_status(ingest_id: str):
    """
    Get ingestion job status and statistics.

    Args:
        ingest_id: Ingestion job identifier

    Returns:
        Job information including status, stats, and errors
    """
    job = job_registry.get_job(ingest_id)

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {ingest_id} not found")

    point_count = track_repo.count_by_ingest_id(ingest_id)
    job['point_count_in_db'] = point_count

    return job


@app.get("/metrics")
async def get_metrics():
    """
    Get Prometheus-compatible metrics.

    Returns basic metrics about ingestion operations.
    """
    if not settings.ENABLE_PROMETHEUS:
        raise HTTPException(status_code=404, detail="Metrics not enabled")

    return {
        "message": "Prometheus metrics endpoint",
        "note": "Full metrics implementation pending"
    }


# Mount the built SPA last so API routes registered above always win. When the
# Vite bundle is missing (local dev, CI), this is a no-op — the API is usable
# on its own. In the Docker image we copy the bundle in so a single container
# serves both halves of the stack.
_web_dist = os.path.abspath(settings.WEB_DIST_DIR)
if os.path.isdir(_web_dist):
    app.mount("/", StaticFiles(directory=_web_dist, html=True), name="web")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level=settings.LOG_LEVEL.lower()
    )
