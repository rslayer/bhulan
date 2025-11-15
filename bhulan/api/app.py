"""
FastAPI application for GPS data ingestion.

Provides REST API endpoints for webhook ingestion, job status, and health checks.
"""

from fastapi import FastAPI, HTTPException, Header, Query, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any, Union
import uuid
from bhulan.config.settings import settings
from bhulan.models.canonical import NormalizationResult
from bhulan.ingestion.normalize import normalize_batch, MappingPlan
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.models.vendor.generic import create_generic_mapping
from bhulan.models.vendor.geotab import create_geotab_mapping
from bhulan.models.vendor.samsara import create_samsara_mapping


app = FastAPI(
    title="Bhulan GPS Ingestion API",
    description="REST API for ingesting GPS data from various sources",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

track_repo = MongoTrackPointRepository()
job_registry = MongoJobRegistry()


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level=settings.LOG_LEVEL.lower()
    )
