"""
Unit tests for normalization module.
"""

import pytest
from datetime import datetime
from bhulan.ingestion.normalize import (
    MappingPlan,
    convert_speed_to_mps,
    convert_altitude_to_meters,
    apply_mapping,
    normalize_record,
    normalize_batch
)
from bhulan.ingestion.validate import ValidationError


class TestUnitConversions:
    """Test unit conversion functions."""
    
    def test_speed_kph_to_mps(self):
        """Test conversion from km/h to m/s."""
        result = convert_speed_to_mps(36.0, 'kph')
        assert abs(result - 10.0) < 0.01
    
    def test_speed_mph_to_mps(self):
        """Test conversion from mph to m/s."""
        result = convert_speed_to_mps(60.0, 'mph')
        assert abs(result - 26.82) < 0.01
    
    def test_speed_mps_to_mps(self):
        """Test m/s stays as m/s."""
        result = convert_speed_to_mps(10.0, 'mps')
        assert result == 10.0
    
    def test_altitude_ft_to_m(self):
        """Test conversion from feet to meters."""
        result = convert_altitude_to_meters(100.0, 'ft')
        assert abs(result - 30.48) < 0.01
    
    def test_altitude_m_to_m(self):
        """Test meters stays as meters."""
        result = convert_altitude_to_meters(100.0, 'm')
        assert result == 100.0


class TestMappingPlan:
    """Test mapping plan application."""
    
    def test_apply_basic_mapping(self):
        """Test basic field mapping."""
        mapping = MappingPlan(
            field_map={'device': 'device_id', 'time': 'ts_utc'},
            vendor='test'
        )
        
        record = {'device': 'TRK-101', 'time': '2024-05-01T12:00:00Z'}
        result = apply_mapping(record, mapping)
        
        assert result['device_id'] == 'TRK-101'
        assert result['ts_utc'] == '2024-05-01T12:00:00Z'
        assert result['src'] == 'test'
    
    def test_apply_mapping_with_defaults(self):
        """Test mapping with default values."""
        mapping = MappingPlan(
            field_map={'device': 'device_id'},
            defaults={'src': 'default_source'},
            vendor='test'
        )
        
        record = {'device': 'TRK-101'}
        result = apply_mapping(record, mapping)
        
        assert result['device_id'] == 'TRK-101'
        assert result['src'] == 'test'
    
    def test_apply_mapping_with_unit_conversion(self):
        """Test mapping with unit conversion."""
        mapping = MappingPlan(
            field_map={'speed': 'speed_mps'},
            unit_map={'speed_mps': 'kph'},
            vendor='test'
        )
        
        record = {'speed': 36.0}
        result = apply_mapping(record, mapping)
        
        assert abs(result['speed_mps'] - 10.0) < 0.01


class TestNormalization:
    """Test record normalization."""
    
    def test_normalize_valid_record(self):
        """Test normalization of valid record."""
        mapping = MappingPlan(
            field_map={
                'device_id': 'device_id',
                'timestamp': 'ts_utc',
                'lat': 'lat',
                'lon': 'lon'
            },
            vendor='test'
        )
        
        record = {
            'device_id': 'TRK-101',
            'timestamp': '2024-05-01T12:00:00Z',
            'lat': 37.7749,
            'lon': -122.4194
        }
        
        point = normalize_record(record, mapping, 'test-ingest-id', seq_no=0)
        
        assert point.device_id == 'TRK-101'
        assert point.lat == 37.7749
        assert point.lon == -122.4194
        assert point.ingest_id == 'test-ingest-id'
        assert point.seq_no == 0
    
    def test_normalize_invalid_coordinates(self):
        """Test normalization fails with invalid coordinates."""
        mapping = MappingPlan(
            field_map={
                'device_id': 'device_id',
                'timestamp': 'ts_utc',
                'lat': 'lat',
                'lon': 'lon'
            },
            vendor='test'
        )
        
        record = {
            'device_id': 'TRK-101',
            'timestamp': '2024-05-01T12:00:00Z',
            'lat': 91.0,  # Invalid
            'lon': -122.4194
        }
        
        with pytest.raises(ValidationError):
            normalize_record(record, mapping, 'test-ingest-id')
    
    def test_normalize_batch(self):
        """Test batch normalization."""
        mapping = MappingPlan(
            field_map={
                'device_id': 'device_id',
                'timestamp': 'ts_utc',
                'lat': 'lat',
                'lon': 'lon'
            },
            vendor='test'
        )
        
        records = [
            {
                'device_id': 'TRK-101',
                'timestamp': '2024-05-01T12:00:00Z',
                'lat': 37.7749,
                'lon': -122.4194
            },
            {
                'device_id': 'TRK-102',
                'timestamp': '2024-05-01T12:01:00Z',
                'lat': 37.7750,
                'lon': -122.4195
            }
        ]
        
        result, points = normalize_batch(records, mapping, 'test-ingest-id')
        
        assert result.accepted == 2
        assert result.rejected == 0
        assert len(points) == 2
        assert result.ingest_id == 'test-ingest-id'
    
    def test_normalize_batch_with_errors(self):
        """Test batch normalization with some invalid records."""
        mapping = MappingPlan(
            field_map={
                'device_id': 'device_id',
                'timestamp': 'ts_utc',
                'lat': 'lat',
                'lon': 'lon'
            },
            vendor='test'
        )
        
        records = [
            {
                'device_id': 'TRK-101',
                'timestamp': '2024-05-01T12:00:00Z',
                'lat': 37.7749,
                'lon': -122.4194
            },
            {
                'device_id': 'TRK-102',
                'timestamp': '2024-05-01T12:01:00Z',
                'lat': 91.0,  # Invalid
                'lon': -122.4195
            }
        ]
        
        result, points = normalize_batch(records, mapping, 'test-ingest-id')
        
        assert result.accepted == 1
        assert result.rejected == 1
        assert len(points) == 1
        assert 1 in result.errors
