"""
Normalization pipeline for GPS data.

Converts raw GPS data from various sources into canonical TrackPoint schema.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
from bhulan.models.canonical import TrackPoint, NormalizationResult
from bhulan.ingestion.validate import (
    validate_required_fields,
    validate_coordinates,
    validate_timestamp,
    validate_speed,
    validate_heading,
    validate_hdop,
    repair_timestamp,
    add_quality_flags,
    ValidationError
)


class MappingPlan:
    """
    Defines how to map source fields to canonical schema.
    
    Includes field mappings, unit conversions, and defaults.
    """
    
    def __init__(
        self,
        field_map: Dict[str, str],
        unit_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict[str, Any]] = None,
        vendor: str = "generic"
    ):
        """
        Initialize mapping plan.
        
        Args:
            field_map: Source field to canonical field mapping
            unit_map: Field to unit type mapping (e.g., {"speed": "kph"})
            defaults: Default values for fields
            vendor: Vendor/source identifier
        """
        self.field_map = field_map
        self.unit_map = unit_map or {}
        self.defaults = defaults or {}
        self.vendor = vendor


def convert_speed_to_mps(value: float, unit: str) -> float:
    """
    Convert speed from various units to meters per second.
    
    Args:
        value: Speed value
        unit: Unit type (kph, mph, mps, knots)
        
    Returns:
        Speed in meters per second
    """
    conversions = {
        'mps': 1.0,
        'kph': 1.0 / 3.6,
        'mph': 0.44704,
        'knots': 0.514444
    }
    
    unit_lower = unit.lower()
    if unit_lower not in conversions:
        raise ValueError(f"Unknown speed unit: {unit}")
    
    return value * conversions[unit_lower]


def convert_altitude_to_meters(value: float, unit: str) -> float:
    """
    Convert altitude from various units to meters.
    
    Args:
        value: Altitude value
        unit: Unit type (m, ft, km)
        
    Returns:
        Altitude in meters
    """
    conversions = {
        'm': 1.0,
        'ft': 0.3048,
        'km': 1000.0
    }
    
    unit_lower = unit.lower()
    if unit_lower not in conversions:
        raise ValueError(f"Unknown altitude unit: {unit}")
    
    return value * conversions[unit_lower]


def apply_mapping(record: Dict[str, Any], mapping: MappingPlan) -> Dict[str, Any]:
    """
    Apply mapping plan to convert source record to canonical fields.
    
    Args:
        record: Source data record
        mapping: Mapping plan to apply
        
    Returns:
        Mapped record with canonical field names
    """
    mapped = {}
    
    for source_field, canonical_field in mapping.field_map.items():
        if source_field in record:
            mapped[canonical_field] = record[source_field]
    
    for field, default_value in mapping.defaults.items():
        if field not in mapped:
            mapped[field] = default_value
    
    if 'speed_mps' in mapped and 'speed_mps' in mapping.unit_map:
        unit = mapping.unit_map['speed_mps']
        if unit != 'mps':
            mapped['speed_mps'] = convert_speed_to_mps(mapped['speed_mps'], unit)
    
    if 'alt_m' in mapped and 'alt_m' in mapping.unit_map:
        unit = mapping.unit_map['alt_m']
        if unit != 'm':
            mapped['alt_m'] = convert_altitude_to_meters(mapped['alt_m'], unit)
    
    mapped['src'] = mapping.vendor
    
    return mapped


def normalize_record(
    record: Dict[str, Any],
    mapping: MappingPlan,
    ingest_id: str,
    seq_no: Optional[int] = None
) -> TrackPoint:
    """
    Normalize a single record to canonical TrackPoint.
    
    Args:
        record: Source data record
        mapping: Mapping plan to apply
        ingest_id: Ingestion job ID
        seq_no: Sequence number within batch
        
    Returns:
        Normalized TrackPoint
        
    Raises:
        ValidationError: If record cannot be normalized
    """
    mapped = apply_mapping(record, mapping)
    
    validate_required_fields(mapped)
    
    mapped['ts_utc'] = repair_timestamp(mapped['ts_utc'])
    
    ts_valid, ts_error = validate_timestamp(mapped['ts_utc'])
    if not ts_valid:
        raise ValidationError(ts_error)
    
    coord_valid, coord_error = validate_coordinates(mapped['lat'], mapped['lon'])
    if not coord_valid:
        raise ValidationError(coord_error)
    
    quality_flags = {}
    
    if 'speed_mps' in mapped and mapped['speed_mps'] is not None:
        mapped['speed_mps'], speed_flagged = validate_speed(mapped['speed_mps'])
        if speed_flagged:
            quality_flags['flag_speed_spike'] = True
    
    if 'heading_deg' in mapped and mapped['heading_deg'] is not None:
        mapped['heading_deg'], heading_flagged = validate_heading(mapped['heading_deg'])
        if heading_flagged:
            quality_flags['flag_bad_heading'] = True
    
    if 'hdop' in mapped and mapped['hdop'] is not None:
        mapped['hdop'], hdop_flagged = validate_hdop(mapped['hdop'])
        if hdop_flagged:
            quality_flags['flag_bad_hdop'] = True
    
    point = TrackPoint(
        device_id=str(mapped['device_id']),
        ts_utc=mapped['ts_utc'],
        lat=float(mapped['lat']),
        lon=float(mapped['lon']),
        speed_mps=mapped.get('speed_mps'),
        heading_deg=mapped.get('heading_deg'),
        alt_m=mapped.get('alt_m'),
        hdop=mapped.get('hdop'),
        src=mapped.get('src'),
        raw={'original': record},
        ingest_id=ingest_id,
        seq_no=seq_no
    )
    
    if quality_flags:
        add_quality_flags(point, quality_flags)
    
    return point


def normalize_batch(
    records: List[Dict[str, Any]],
    mapping: MappingPlan,
    ingest_id: Optional[str] = None
) -> NormalizationResult:
    """
    Normalize a batch of records.
    
    Args:
        records: List of source data records
        mapping: Mapping plan to apply
        ingest_id: Ingestion job ID (generated if not provided)
        
    Returns:
        NormalizationResult with accepted/rejected counts and errors
    """
    if ingest_id is None:
        ingest_id = str(uuid.uuid4())
    
    accepted = []
    rejected = 0
    errors = {}
    
    for idx, record in enumerate(records):
        try:
            point = normalize_record(record, mapping, ingest_id, seq_no=idx)
            accepted.append(point)
        except ValidationError as e:
            rejected += 1
            errors[idx] = str(e)
        except Exception as e:
            rejected += 1
            errors[idx] = f"Unexpected error: {str(e)}"
    
    return NormalizationResult(
        accepted=len(accepted),
        rejected=rejected,
        errors=errors,
        ingest_id=ingest_id
    ), accepted
