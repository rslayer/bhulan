"""
Samsara vendor adapter.

Maps Samsara GPS data format to canonical schema.
"""

from bhulan.ingestion.normalize import MappingPlan


def create_samsara_mapping() -> MappingPlan:
    """
    Create mapping plan for Samsara GPS data.
    
    Samsara format typically includes nested structure:
    - device.id: Device ID
    - timestamp: Epoch milliseconds
    - location.lat, location.lng: Coordinates
    - vehicle.speedKph: Speed in km/h
    
    Returns:
        MappingPlan for Samsara data
    """
    field_map = {
        'device.id': 'device_id',
        'deviceId': 'device_id',
        'timestamp': 'ts_utc',
        'time': 'ts_utc',
        'location.lat': 'lat',
        'location.latitude': 'lat',
        'latitude': 'lat',
        'location.lng': 'lon',
        'location.longitude': 'lon',
        'longitude': 'lon',
        'vehicle.speedKph': 'speed_mps',
        'speedKph': 'speed_mps',
        'speed': 'speed_mps',
        'heading': 'heading_deg',
        'bearing': 'heading_deg',
    }
    
    unit_map = {
        'speed_mps': 'kph',  # Samsara uses km/h
    }
    
    return MappingPlan(
        field_map=field_map,
        unit_map=unit_map,
        defaults={'src': 'samsara'},
        vendor='samsara'
    )
