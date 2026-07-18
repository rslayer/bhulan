"""
FastAPI application for GPS data ingestion.

Provides REST API endpoints for webhook ingestion, job status, and health checks.
"""

import json
import math
import os
import uuid
from typing import Any, Dict, List, Optional, Union

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from bhulan.api.limiter import limiter
from bhulan.api.routes.auth import router as auth_router
from bhulan.api.routes.compare import router as compare_router
from bhulan.api.routes.history import router as history_router
from bhulan.api.routes.insights import router as insights_router
from bhulan.auth import db as auth_db
from bhulan.config.settings import settings
from bhulan.ingestion.normalize import MappingPlan, normalize_batch
from bhulan.models.canonical import NormalizationResult
from bhulan.models.vendor.generic import create_generic_mapping
from bhulan.models.vendor.geotab import create_geotab_mapping
from bhulan.models.vendor.samsara import create_samsara_mapping
from bhulan.storage.mongo_repo import MongoJobRegistry, MongoTrackPointRepository

app = FastAPI(
    title="Bhulan GPS API",
    description="Mobility insights and map plotting for GPS coordinates, "
    "plus ingestion endpoints for vendor feeds.",
    version="2.2.0",
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
# a 429 instead of melting the box. slowapi attaches itself through app.state,
# and the *same* limiter instance is imported by routes/insights.py so its
# exception handler's ``_inject_headers`` call sees the correct state.
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]


def _scrub_non_finite(obj: Any) -> Any:
    """Recursively replace non-finite floats (NaN/inf) with their text form."""
    if isinstance(obj, float):
        return repr(obj) if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _scrub_non_finite(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub_non_finite(v) for v in obj]
    return obj


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    _request: Any, exc: RequestValidationError
) -> JSONResponse:
    """
    Render 422s even when the rejected input is a non-finite float.

    FastAPI's default handler echoes the offending value back in the error
    detail's ``input`` field. When that value is ``NaN``/``Infinity`` (or an
    overflowing numeral like ``1e400`` that stdlib ``json`` parses to ``inf``),
    Starlette's ``JSONResponse`` — which serializes with ``allow_nan=False`` —
    raises while *building* the 422 body, surfacing to the client as an
    unhandled 500. Scrubbing non-finite floats first keeps the 422 clean while
    preserving the default response shape for every ordinary validation error.
    """
    try:
        detail: Any = _scrub_non_finite(jsonable_encoder(exc.errors()))
    except RecursionError:
        # A pathologically deep input (e.g. thousands of nested JSON arrays)
        # is echoed back in each error's ``input`` field; ``jsonable_encoder``
        # recurses once per level and blows the stack while *building* the 422
        # body — surfacing to the client as an unhandled 500. Once the stack
        # unwinds back here there is ample headroom to return a compact,
        # input-free 422 instead of crashing.
        detail = [
            {
                "type": "too_deeply_nested",
                "msg": "Request body is too deeply nested to process.",
            }
        ]
    return JSONResponse(status_code=422, content={"detail": detail})

app.include_router(insights_router)
app.include_router(compare_router)
app.include_router(auth_router)
app.include_router(history_router)

# Best-effort DB init at startup. The routes also call init_db() lazily so
# tests can point at a tempfile without reimporting this module, but when
# the feature is enabled on a real deploy we want the file + schema to
# exist before the first request lands.
if settings.BHULAN_AUTH_ENABLED:
    if settings.BHULAN_AUTH_SECRET == "dev-secret-change-me":
        raise RuntimeError(
            "BHULAN_AUTH_ENABLED is true but BHULAN_AUTH_SECRET is still the "
            "default value. Set BHULAN_AUTH_SECRET to a long random string "
            "before enabling authentication."
        )
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


@app.get("/v1/capabilities", tags=["insights"])
async def capabilities() -> Dict[str, bool]:
    """Feature flags the frontend can use without probing disabled routes."""
    return {
        "auth_enabled": settings.BHULAN_AUTH_ENABLED,
        "history_enabled": settings.BHULAN_AUTH_ENABLED,
        "reverse_geocoding_enabled": settings.ENABLE_REVERSE_GEOCODING,
        "public_demo": not settings.BHULAN_AUTH_ENABLED,
    }


def _maybe_repo() -> Optional["MongoTrackPointRepository"]:
    """
    Build a Mongo repo lazily and ensure production indexes exist.

    The insights endpoints don't need Mongo, and we don't want the service to
    fail to start when Mongo isn't available. Ingestion endpoints handle a
    missing repo by returning 503.
    """
    try:
        repo = MongoTrackPointRepository()
        repo.create_indexes()
        return repo
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


def _mapping_from_header(raw: Optional[str], fallback: MappingPlan) -> MappingPlan:
    """Parse an optional JSON mapping override from X-Bhulan-Mapping.

    Expected shape: {"field_map": {...}, "unit_map": {...}, "defaults": {...}, "vendor": "custom"}.
    Missing keys fall back to the selected vendor mapping.
    """
    if not raw:
        return fallback
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid X-Bhulan-Mapping JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="X-Bhulan-Mapping must be a JSON object")

    field_map = data.get("field_map", fallback.field_map)
    unit_map = data.get("unit_map", fallback.unit_map)
    defaults = data.get("defaults", fallback.defaults)
    vendor = data.get("vendor", fallback.vendor)
    if not isinstance(field_map, dict):
        raise HTTPException(status_code=400, detail="X-Bhulan-Mapping.field_map must be an object")
    if not isinstance(unit_map, dict):
        raise HTTPException(status_code=400, detail="X-Bhulan-Mapping.unit_map must be an object")
    if not isinstance(defaults, dict):
        raise HTTPException(status_code=400, detail="X-Bhulan-Mapping.defaults must be an object")
    if not isinstance(vendor, str):
        raise HTTPException(status_code=400, detail="X-Bhulan-Mapping.vendor must be a string")

    return MappingPlan(
        field_map={str(k): str(v) for k, v in field_map.items()},
        unit_map={str(k): str(v) for k, v in unit_map.items()},
        defaults=defaults,
        vendor=vendor,
    )


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
async def get_config(_: None = Depends(verify_api_key)):
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
    _: None = Depends(verify_api_key),
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
    if track_repo is None or job_registry is None:
        raise HTTPException(status_code=503, detail="MongoDB is not available")

    if ingest_id is None:
        ingest_id = str(uuid.uuid4())

    if isinstance(payload, dict):
        records = [payload]
    else:
        records = payload

    if len(records) > settings.MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"Too many records: {len(records)} > {settings.MAX_BATCH_SIZE}",
        )

    if vendor == "geotab":
        mapping = create_geotab_mapping()
    elif vendor == "samsara":
        mapping = create_samsara_mapping()
    else:
        mapping = create_generic_mapping()
    mapping = _mapping_from_header(x_bhulan_mapping, mapping)

    job_registry.create_job(
        ingest_id=ingest_id,
        source="webhook",
        params={"vendor": vendor, "record_count": len(records)},
    )

    try:
        result, points = normalize_batch(records, mapping, ingest_id)

        if points:
            track_repo.upsert_batch(points)

        job_registry.update_job_status(
            ingest_id=ingest_id,
            status="succeeded" if result.rejected == 0 else "partial",
            stats={"read": len(records), "accepted": result.accepted, "rejected": result.rejected},
            error_sample=dict(list(result.errors.items())[:10]),
        )

        return result

    except Exception as e:
        job_registry.update_job_status(
            ingest_id=ingest_id, status="failed", error_sample={0: str(e)}
        )
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@app.get("/jobs/{ingest_id}")
async def get_job_status(ingest_id: str, _: None = Depends(verify_api_key)):
    """
    Get ingestion job status and statistics.

    Args:
        ingest_id: Ingestion job identifier

    Returns:
        Job information including status, stats, and errors
    """
    if track_repo is None or job_registry is None:
        raise HTTPException(status_code=503, detail="MongoDB is not available")

    job = job_registry.get_job(ingest_id)

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {ingest_id} not found")

    point_count = track_repo.count_by_ingest_id(ingest_id)
    job["point_count_in_db"] = point_count

    return job


@app.get("/metrics", response_class=PlainTextResponse)
async def get_metrics(_: None = Depends(verify_api_key)) -> str:
    """Get Prometheus-compatible process health metrics."""
    if not settings.ENABLE_PROMETHEUS:
        raise HTTPException(status_code=404, detail="Metrics not enabled")

    mongo_available = 0
    if track_repo is not None:
        try:
            track_repo.collection.database.client.server_info()
            mongo_available = 1
        except Exception:
            mongo_available = 0

    auth_enabled = 1 if settings.BHULAN_AUTH_ENABLED else 0
    return (
        "# HELP bhulan_up Whether the Bhulan API process is serving requests.\n"
        "# TYPE bhulan_up gauge\n"
        "bhulan_up 1\n"
        "# HELP bhulan_mongo_available Whether MongoDB is reachable for ingestion.\n"
        "# TYPE bhulan_mongo_available gauge\n"
        f"bhulan_mongo_available {mongo_available}\n"
        "# HELP bhulan_auth_enabled Whether optional auth/history is enabled.\n"
        "# TYPE bhulan_auth_enabled gauge\n"
        f"bhulan_auth_enabled {auth_enabled}\n"
    )


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
        app, host=settings.API_HOST, port=settings.API_PORT, log_level=settings.LOG_LEVEL.lower()
    )
