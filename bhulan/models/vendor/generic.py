"""
Generic vendor adapter for standard GPS data formats.

Handles common field names and formats used across various GPS providers.
"""

from typing import Dict, Any
from bhulan.ingestion.normalize import MappingPlan


def create_generic_mapping() -> MappingPlan:
    """
    Create mapping plan for generic GPS data.
    
    Supports common field name variations:
    - device_id: device, unit, vehicle_id, tracker_id, asset_id
    - ts_utc: timestamp, time, datetime, event_time, ts, time_utc
    - lat: lat, latitude
    - lon: lon, lng, long, longitude
    - speed_mps: speed, speed_kph, speed_mph, velocity
    - heading_deg: heading, course, bearing, direction
    - alt_m: altitude, alt, elevation
    - hdop: hdop, accuracy, precision
    
    Returns:
        MappingPlan for generic GPS data
    """
    field_map = {
        'device_id': 'device_id',
        'device': 'device_id',
        'unit': 'device_id',
        'vehicle_id': 'device_id',
        'tracker_id': 'device_id',
        'asset_id': 'device_id',
        
        'timestamp': 'ts_utc',
        'time': 'ts_utc',
        'datetime': 'ts_utc',
        'event_time': 'ts_utc',
        'ts': 'ts_utc',
        'time_utc': 'ts_utc',
        'ts_utc': 'ts_utc',
        
        'lat': 'lat',
        'latitude': 'lat',
        
        'lon': 'lon',
        'lng': 'lon',
        'long': 'lon',
        'longitude': 'lon',
        
        'speed': 'speed_mps',
        'speed_kph': 'speed_mps',
        'speed_mph': 'speed_mps',
        'velocity': 'speed_mps',
        'speed_mps': 'speed_mps',
        
        'heading': 'heading_deg',
        'course': 'heading_deg',
        'bearing': 'heading_deg',
        'direction': 'heading_deg',
        'heading_deg': 'heading_deg',
        
        'altitude': 'alt_m',
        'alt': 'alt_m',
        'elevation': 'alt_m',
        'alt_m': 'alt_m',
        
        'hdop': 'hdop',
        'accuracy': 'hdop',
        'precision': 'hdop',
    }
    
    unit_map = {}
    
    return MappingPlan(
        field_map=field_map,
        unit_map=unit_map,
        defaults={'src': 'generic'},
        vendor='generic'
    )


def infer_field_mapping(headers: list) -> Dict[str, str]:
    """
    Infer field mapping from CSV/Excel headers using fuzzy matching.
    
    Args:
        headers: List of header names from file
        
    Returns:
        Dictionary mapping source headers to canonical fields
    """
    normalized = {}
    for header in headers:
        norm = header.lower().strip().replace(' ', '_').replace('-', '_')
        normalized[header] = norm
    
    generic = create_generic_mapping()
    
    mapping = {}
    for original_header, norm_header in normalized.items():
        if norm_header in generic.field_map:
            canonical = generic.field_map[norm_header]
            mapping[original_header] = canonical
    
    return mapping
