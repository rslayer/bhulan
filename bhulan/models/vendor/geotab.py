"""
Geotab vendor adapter.

Maps Geotab GPS data format to canonical schema.
"""

from bhulan.ingestion.normalize import MappingPlan


def create_geotab_mapping() -> MappingPlan:
    """
    Create mapping plan for Geotab GPS data.
    
    Geotab format typically includes:
    - id: Device ID
    - dateTime: ISO timestamp
    - latitude, longitude: Coordinates
    - speed: Speed in km/h
    - bearing: Heading in degrees
    
    Returns:
        MappingPlan for Geotab data
    """
    field_map = {
        'id': 'device_id',
        'deviceId': 'device_id',
        'dateTime': 'ts_utc',
        'timestamp': 'ts_utc',
        'latitude': 'lat',
        'longitude': 'lon',
        'speed': 'speed_mps',
        'bearing': 'heading_deg',
        'altitude': 'alt_m',
    }
    
    unit_map = {
        'speed_mps': 'kph',  # Geotab uses km/h
    }
    
    return MappingPlan(
        field_map=field_map,
        unit_map=unit_map,
        defaults={'src': 'geotab'},
        vendor='geotab'
    )
