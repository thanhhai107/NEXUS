"""Tests for Bronze schema validation."""

from __future__ import annotations

import pytest

from governance.quality.bronze_validation import (
    BronzeSchemaValidator,
    ValidationResult,
    BatchValidationResult,
    validate_bronze_records,
    validate_streaming_bronze,
)


class TestValidationResult:
    """Tests for ValidationResult."""

    def test_valid_record(self):
        """Test validation of valid record."""
        validator = BronzeSchemaValidator("test", "run1")
        
        record = {"id": 1, "name": "Alice", "age": 30}
        result = validator.validate_record(record)
        
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_required_field(self):
        """Test validation with missing required field."""
        validator = BronzeSchemaValidator("openaq", "run1")
        validator.required_fields = ["location", "parameter"]
        
        record = {"location": "London", "value": 25}
        result = validator.validate_record(record)
        
        assert not result.is_valid
        assert any("parameter" in e for e in result.errors)

    def test_invalid_type(self):
        """Test validation with invalid type."""
        validator = BronzeSchemaValidator("test", "run1")
        
        record = {"latitude": "not_a_number"}
        result = validator.validate_record(record)
        
        assert not result.is_valid
        assert any("latitude" in e for e in result.errors)

    def test_invalid_coordinate(self):
        """Test validation with out-of-range coordinate."""
        validator = BronzeSchemaValidator("test", "run1")
        
        record = {"latitude": 100}  # Invalid: > 90
        result = validator.validate_record(record)
        
        # Out of range coordinates produce warnings, not errors
        # unless strictly invalid
        assert len(result.warnings) > 0
        assert any("Latitude" in w for w in result.warnings)

    def test_warnings_for_edge_cases(self):
        """Test that warnings are generated for edge cases."""
        validator = BronzeSchemaValidator("test", "run1")
        
        record = {"latitude": 89.5, "longitude": 179.5}  # Edge but valid
        result = validator.validate_record(record)
        
        assert result.is_valid
        assert len(result.warnings) == 0


class TestBatchValidationResult:
    """Tests for BatchValidationResult."""

    def test_valid_ratio_calculation(self):
        """Test valid ratio calculation."""
        result = BatchValidationResult(source_id="test", run_id="run1")
        result.total_records = 100
        result.valid_records = 95
        
        assert result.valid_ratio == 0.95

    def test_status_passed(self):
        """Test status with high valid ratio."""
        result = BatchValidationResult(source_id="test", run_id="run1")
        result.total_records = 100
        result.valid_records = 96
        
        assert result.status == "passed"

    def test_status_warning(self):
        """Test status with medium valid ratio."""
        result = BatchValidationResult(source_id="test", run_id="run1")
        result.total_records = 100
        result.valid_records = 85
        
        assert result.status == "warning"

    def test_status_failed(self):
        """Test status with low valid ratio."""
        result = BatchValidationResult(source_id="test", run_id="run1")
        result.total_records = 100
        result.valid_records = 70
        
        assert result.status == "failed"

    def test_empty_batch(self):
        """Test with empty batch."""
        result = BatchValidationResult(source_id="test", run_id="run1")
        
        assert result.valid_ratio == 1.0
        assert result.status == "passed"


class TestBronzeSchemaValidator:
    """Tests for BronzeSchemaValidator."""

    def test_validate_records_batch(self):
        """Test batch validation of records."""
        validator = BronzeSchemaValidator("openaq", "run1")
        # Use simpler required fields for test
        validator.required_fields = []
        
        records = [
            {"location": "London", "parameter": "pm25", "value": 25},
            {"location": "Paris", "parameter": "pm25", "value": 30},
            {"location": "Berlin"},  # Missing fields but no required set
        ]
        
        result = validator.validate_records(records, quarantine_invalid=False)
        
        assert result.total_records == 3
        assert result.valid_records == 3
        assert result.invalid_records == 0
        assert result.status == "passed"

    def test_validate_streaming(self):
        """Test streaming validation."""
        validator = BronzeSchemaValidator("tfl_arrivals", "run1")
        # No required fields for this test
        validator.required_fields = []
        
        records = [
            {"stop_id": "1", "line_id": "N", "dest": "North"},
            {"stop_id": "2"},  # No required fields so valid
            {"stop_id": "3", "line_id": "S", "dest": "South"},
        ]
        
        valid = list(validator.validate_streaming(records))
        
        assert len(valid) == 3

    def test_malformed_json_in_string(self):
        """Test detection of malformed JSON in string field."""
        validator = BronzeSchemaValidator("test", "run1")
        
        record = {"id": 1, "data": '{"key": "val', "extra": 123}
        result = validator.validate_record(record)
        
        # Should catch malformed JSON
        assert not result.is_valid

    def test_get_validation_stats(self):
        """Test getting validation statistics."""
        validator = BronzeSchemaValidator("test", "run1")
        # Add a required field to trigger errors
        validator.required_fields = ["id"]
        
        validator.validate_records([
            {"id": "1"},  # Valid
        ], quarantine_invalid=False)
        
        stats = validator.get_validation_stats()
        
        assert "total_errors" in stats


class TestValidateFunctions:
    """Tests for convenience functions."""

    def test_validate_bronze_records_function(self):
        """Test validate_bronze_records convenience function."""
        records = [
            {"location": "London", "parameter": "pm25", "value": 25},
            {"location": "Paris"},
        ]
        
        result = validate_bronze_records("unknown_source", "run1", records, quarantine=False)
        
        assert result.total_records == 2

    def test_validate_streaming_bronze_function(self):
        """Test validate_streaming_bronze convenience function."""
        records = [
            {"stop_id": "1", "line_id": "N"},
            {"stop_id": "2"},
            {"stop_id": "3", "line_id": "S"},
        ]
        
        valid = list(validate_streaming_bronze("unknown_source", "run1", records))
        
        assert len(valid) == 3
