"""
Validation rules for GPS data quality.

Implements data quality checks and repairs for ingested GPS data.
"""

from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from bhulan.models.canonical import TrackPoint


class ValidationError(Exception):
    """Raised when validation fails for a data point."""
    pass


def validate_required_fields(record: Dict[str, Any]) -> None:
    """
    Validate that required fields are present and non-empty.
    
    Args:
        record: Raw data record
        
    Raises:
        ValidationError: If required fields are missing or invalid
    """
    required = ['device_id', 'ts_utc', 'lat', 'lon']
    
    for field in required:
        if field not in record or record[field] is None:
            raise ValidationError(f"Missing required field: {field}")
        
        if field == 'device_id' and not str(record[field]).strip():
            raise ValidationError("device_id cannot be empty")


def validate_coordinates(lat: float, lon: float) -> Tuple[bool, Optional[str]]:
    """
    Validate latitude and longitude are within valid ranges.
    
    Args:
        lat: Latitude value
        lon: Longitude value
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not (-90 <= lat <= 90):
        return False, f"Latitude {lat} out of range [-90, 90]"
    
    if not (-180 <= lon <= 180):
        return False, f"Longitude {lon} out of range [-180, 180]"
    
    return True, None


def validate_timestamp(ts: datetime) -> Tuple[bool, Optional[str]]:
    """
    Validate timestamp is within reasonable range.
    
    Args:
        ts: Timestamp to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    min_date = datetime(1970, 1, 1)
    max_date = datetime.utcnow() + timedelta(days=2)
    
    if ts < min_date:
        return False, f"Timestamp {ts} is before 1970-01-01"
    
    if ts > max_date:
        return False, f"Timestamp {ts} is more than 2 days in the future"
    
    return True, None


def validate_speed(speed_mps: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    Validate and cap speed at reasonable maximum.
    
    Args:
        speed_mps: Speed in meters per second
        
    Returns:
        Tuple of (validated_speed, was_flagged)
    """
    if speed_mps is None:
        return None, False
    
    if speed_mps < 0:
        return None, True  # Negative speed is invalid
    
    if speed_mps > 120:
        return None, True  # Unreasonable speed
    
    return speed_mps, False


def validate_heading(heading_deg: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    Validate heading is within valid range.
    
    Args:
        heading_deg: Heading in degrees
        
    Returns:
        Tuple of (validated_heading, was_flagged)
    """
    if heading_deg is None:
        return None, False
    
    if not (0 <= heading_deg < 360):
        return None, True  # Invalid heading
    
    return heading_deg, False


def validate_hdop(hdop: Optional[float]) -> Tuple[Optional[float], bool]:
    """
    Validate HDOP (Horizontal Dilution of Precision).
    
    Args:
        hdop: HDOP value
        
    Returns:
        Tuple of (validated_hdop, was_flagged)
    """
    if hdop is None:
        return None, False
    
    if hdop < 0:
        return None, True  # Negative HDOP is invalid
    
    if hdop > 10:
        return hdop, True
    
    return hdop, False


def repair_timestamp(value: Any) -> datetime:
    """
    Attempt to repair/parse timestamp from various formats.
    
    Args:
        value: Timestamp value (string, int, float, or datetime)
        
    Returns:
        Parsed datetime in UTC
        
    Raises:
        ValidationError: If timestamp cannot be parsed
    """
    if isinstance(value, datetime):
        return value
    
    if isinstance(value, (int, float)):
        try:
            if value > 1e10:  # Likely milliseconds
                return datetime.utcfromtimestamp(value / 1000)
            else:  # Likely seconds
                return datetime.utcfromtimestamp(value)
        except (ValueError, OSError) as e:
            raise ValidationError(f"Invalid epoch timestamp: {value}")
    
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            pass
        
        try:
            from dateutil import parser
            return parser.parse(value)
        except Exception as e:
            raise ValidationError(f"Cannot parse timestamp: {value}")
    
    raise ValidationError(f"Unsupported timestamp type: {type(value)}")


def add_quality_flags(point: TrackPoint, flags: Dict[str, bool]) -> None:
    """
    Add quality flags to track point metadata.
    
    Args:
        point: TrackPoint to add flags to
        flags: Dictionary of flag names to boolean values
    """
    if point.raw is None:
        point.raw = {}
    
    if 'meta' not in point.raw:
        point.raw['meta'] = {}
    
    point.raw['meta']['quality_flags'] = flags
