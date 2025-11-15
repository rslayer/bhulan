"""
Unit tests for validation module.
"""

import pytest
from datetime import datetime, timedelta
from bhulan.ingestion.validate import (
    validate_required_fields,
    validate_coordinates,
    validate_timestamp,
    validate_speed,
    validate_heading,
    validate_hdop,
    repair_timestamp,
    ValidationError
)


class TestRequiredFields:
    """Test required field validation."""
    
    def test_valid_required_fields(self):
        """Test validation passes with all required fields."""
        record = {
            'device_id': 'TRK-101',
            'ts_utc': datetime.utcnow(),
            'lat': 37.7749,
            'lon': -122.4194
        }
        validate_required_fields(record)
    
    def test_missing_device_id(self):
        """Test validation fails when device_id is missing."""
        record = {
            'ts_utc': datetime.utcnow(),
            'lat': 37.7749,
            'lon': -122.4194
        }
        with pytest.raises(ValidationError, match="Missing required field: device_id"):
            validate_required_fields(record)
    
    def test_empty_device_id(self):
        """Test validation fails when device_id is empty."""
        record = {
            'device_id': '   ',
            'ts_utc': datetime.utcnow(),
            'lat': 37.7749,
            'lon': -122.4194
        }
        with pytest.raises(ValidationError, match="device_id cannot be empty"):
            validate_required_fields(record)


class TestCoordinateValidation:
    """Test coordinate validation."""
    
    def test_valid_coordinates(self):
        """Test validation passes with valid coordinates."""
        valid, error = validate_coordinates(37.7749, -122.4194)
        assert valid is True
        assert error is None
    
    def test_latitude_out_of_range_high(self):
        """Test validation fails with latitude > 90."""
        valid, error = validate_coordinates(91.0, -122.4194)
        assert valid is False
        assert "out of range" in error
    
    def test_latitude_out_of_range_low(self):
        """Test validation fails with latitude < -90."""
        valid, error = validate_coordinates(-91.0, -122.4194)
        assert valid is False
        assert "out of range" in error
    
    def test_longitude_out_of_range_high(self):
        """Test validation fails with longitude > 180."""
        valid, error = validate_coordinates(37.7749, 181.0)
        assert valid is False
        assert "out of range" in error
    
    def test_longitude_out_of_range_low(self):
        """Test validation fails with longitude < -180."""
        valid, error = validate_coordinates(37.7749, -181.0)
        assert valid is False
        assert "out of range" in error


class TestTimestampValidation:
    """Test timestamp validation."""
    
    def test_valid_timestamp(self):
        """Test validation passes with valid timestamp."""
        ts = datetime.utcnow()
        valid, error = validate_timestamp(ts)
        assert valid is True
        assert error is None
    
    def test_timestamp_too_old(self):
        """Test validation fails with timestamp before 1970."""
        ts = datetime(1969, 12, 31)
        valid, error = validate_timestamp(ts)
        assert valid is False
        assert "before 1970" in error
    
    def test_timestamp_too_future(self):
        """Test validation fails with timestamp too far in future."""
        ts = datetime.utcnow() + timedelta(days=10)
        valid, error = validate_timestamp(ts)
        assert valid is False
        assert "future" in error


class TestSpeedValidation:
    """Test speed validation."""
    
    def test_valid_speed(self):
        """Test validation passes with valid speed."""
        speed, flagged = validate_speed(30.0)
        assert speed == 30.0
        assert flagged is False
    
    def test_negative_speed(self):
        """Test validation flags negative speed."""
        speed, flagged = validate_speed(-10.0)
        assert speed is None
        assert flagged is True
    
    def test_excessive_speed(self):
        """Test validation flags excessive speed."""
        speed, flagged = validate_speed(150.0)
        assert speed is None
        assert flagged is True
    
    def test_none_speed(self):
        """Test validation handles None speed."""
        speed, flagged = validate_speed(None)
        assert speed is None
        assert flagged is False


class TestHeadingValidation:
    """Test heading validation."""
    
    def test_valid_heading(self):
        """Test validation passes with valid heading."""
        heading, flagged = validate_heading(90.0)
        assert heading == 90.0
        assert flagged is False
    
    def test_invalid_heading_negative(self):
        """Test validation flags negative heading."""
        heading, flagged = validate_heading(-10.0)
        assert heading is None
        assert flagged is True
    
    def test_invalid_heading_high(self):
        """Test validation flags heading >= 360."""
        heading, flagged = validate_heading(360.0)
        assert heading is None
        assert flagged is True


class TestTimestampRepair:
    """Test timestamp repair/parsing."""
    
    def test_repair_datetime(self):
        """Test repair handles datetime objects."""
        dt = datetime.utcnow()
        result = repair_timestamp(dt)
        assert result == dt
    
    def test_repair_epoch_seconds(self):
        """Test repair handles epoch seconds."""
        epoch = 1714568400  # May 1, 2024
        result = repair_timestamp(epoch)
        assert isinstance(result, datetime)
        assert result.year == 2024
    
    def test_repair_epoch_milliseconds(self):
        """Test repair handles epoch milliseconds."""
        epoch_ms = 1714568400000  # May 1, 2024 in milliseconds
        result = repair_timestamp(epoch_ms)
        assert isinstance(result, datetime)
        assert result.year == 2024
    
    def test_repair_iso_string(self):
        """Test repair handles ISO format strings."""
        iso_str = "2024-05-01T12:00:00Z"
        result = repair_timestamp(iso_str)
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 5
        assert result.day == 1
    
    def test_repair_invalid_type(self):
        """Test repair fails with invalid type."""
        with pytest.raises(ValidationError):
            repair_timestamp([1, 2, 3])
