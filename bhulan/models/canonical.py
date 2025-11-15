"""
Canonical schema for GPS track points.

Defines the normalized data model that all ingested GPS data is converted to.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


class TrackPoint(BaseModel):
    """
    Canonical GPS track point model.
    
    All ingested GPS data is normalized to this schema regardless of source.
    """
    device_id: str = Field(..., description="Unique identifier for the device/vehicle")
    ts_utc: datetime = Field(..., description="Timestamp in UTC")
    lat: float = Field(..., ge=-90, le=90, description="Latitude in decimal degrees")
    lon: float = Field(..., ge=-180, le=180, description="Longitude in decimal degrees")
    
    speed_mps: Optional[float] = Field(None, ge=0, description="Speed in meters per second")
    heading_deg: Optional[float] = Field(None, ge=0, lt=360, description="Heading in degrees (0-359)")
    alt_m: Optional[float] = Field(None, description="Altitude in meters")
    hdop: Optional[float] = Field(None, ge=0, description="Horizontal dilution of precision")
    
    src: Optional[str] = Field(None, description="Source/vendor identifier")
    raw: Optional[Dict[str, Any]] = Field(None, description="Original raw data")
    ingest_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Ingestion job ID")
    seq_no: Optional[int] = Field(None, description="Sequence number within source")
    
    @field_validator('ts_utc')
    @classmethod
    def validate_timestamp(cls, v):
        """Ensure timestamp is reasonable (not too far in past or future)."""
        min_date = datetime(1970, 1, 1)
        max_date = datetime.now() + timedelta(days=2)
        if v < min_date or v > max_date:
            raise ValueError(f"Timestamp {v} is outside valid range [{min_date}, {max_date}]")
        return v
    
    @field_validator('speed_mps')
    @classmethod
    def validate_speed(cls, v):
        """Cap speed at reasonable maximum (120 m/s = 432 km/h)."""
        if v is not None and v > 120:
            return None  # Set to None if unreasonable
        return v
    
    def to_mongo_doc(self) -> Dict[str, Any]:
        """
        Convert to MongoDB document format with GeoJSON location.
        
        Returns:
            Dictionary suitable for MongoDB insertion with 2dsphere index support
        """
        doc = self.model_dump()
        doc['loc'] = {
            'type': 'Point',
            'coordinates': [self.lon, self.lat]  # GeoJSON uses [lon, lat] order
        }
        return doc
    
    def compute_hash(self) -> str:
        """
        Compute deterministic hash for deduplication.
        
        Uses device_id, timestamp, lat, lon to create unique identifier.
        """
        import hashlib
        key = f"{self.device_id}:{self.ts_utc.isoformat()}:{self.lat:.6f}:{self.lon:.6f}"
        return hashlib.sha256(key.encode()).hexdigest()


class NormalizationResult(BaseModel):
    """Result of normalizing a batch of GPS data."""
    accepted: int = Field(..., description="Number of records accepted")
    rejected: int = Field(..., description="Number of records rejected")
    errors: Dict[int, str] = Field(default_factory=dict, description="Row index to error message mapping")
    ingest_id: str = Field(..., description="Ingestion job ID")


from datetime import timedelta
