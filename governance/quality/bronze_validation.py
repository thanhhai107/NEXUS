"""Bronze Layer Schema Validation.

Validates records at ingestion time to catch schema violations early
and route invalid records to quarantine before they enter the lake.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from common.config import RUNTIME_DIR
from governance.quality.quarantine import quarantine_records


logger = logging.getLogger(__name__)


BRONZE_VALIDATION_DIR = RUNTIME_DIR / "validation" / "bronze"


@dataclass
class ValidationResult:
    """Result of a single record validation."""
    record: dict[str, Any]
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    schema_version: str | None = None


@dataclass
class BatchValidationResult:
    """Result of batch validation."""
    source_id: str
    run_id: str
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    warnings_count: int = 0
    validation_errors: list[dict[str, Any]] = field(default_factory=list)
    quarantined_path: Path | None = None
    
    @property
    def valid_ratio(self) -> float:
        if self.total_records == 0:
            return 1.0
        return self.valid_records / self.total_records
    
    @property
    def status(self) -> str:
        if self.valid_ratio >= 0.95:
            return "passed"
        elif self.valid_ratio >= 0.80:
            return "warning"
        else:
            return "failed"
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "valid_ratio": round(self.valid_ratio, 4),
            "status": self.status,
            "quarantined_path": str(self.quarantined_path) if self.quarantined_path else None,
        }


class BronzeSchemaValidator:
    """Validates records against expected schema at Bronze layer."""
    
    # Default required fields by source type
    REQUIRED_FIELDS: dict[str, list[str]] = {
        "tfl_arrivals": ["stop_id", "line_id", "timestamp", "expected_arrival"],
        "tfl_line_status": ["line_id", "status_timestamp", "status"],
        "openaq": ["location", "parameter", "value", "date_utc"],
        "waqi": ["station", "time", "aqi"],
        "londonair": ["site_code", "species", "measurement_date", "value"],
        "openweather": ["lat", "lon", "dt", "temp"],
        "openmeteo": ["latitude", "longitude", "hour"],
        "dft": ["count_point_id", "year", "count_date"],
        "stats19": ["accident_index", "date", "location"],
    }
    
    # Field type expectations
    FIELD_TYPES: dict[str, str] = {
        "latitude": "number",
        "lat": "number",
        "longitude": "number",
        "lon": "number",
        "lng": "number",
        "timestamp": "string",
        "dt": "integer",
        "value": "number",
        "aqi": "integer",
        "year": "integer",
        "date": "string",
    }
    
    def __init__(
        self,
        source_id: str,
        run_id: str,
        validation_dir: Path | None = None,
    ):
        """Initialize validator.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier
            validation_dir: Directory for validation results
        """
        self.source_id = source_id
        self.run_id = run_id
        self.validation_dir = validation_dir or BRONZE_VALIDATION_DIR
        self.validation_dir.mkdir(parents=True, exist_ok=True)
        
        self.required_fields = self.REQUIRED_FIELDS.get(source_id, [])
        self._validation_errors: list[dict[str, Any]] = []
    
    def validate_record(self, record: dict[str, Any]) -> ValidationResult:
        """Validate a single record.
        
        Args:
            record: Record to validate
            
        Returns:
            ValidationResult with validity and errors
        """
        errors = []
        warnings = []
        
        # Check required fields
        for field_name in self.required_fields:
            if field_name not in record:
                errors.append(f"Missing required field: {field_name}")
            elif record[field_name] is None or record[field_name] == "":
                errors.append(f"Empty required field: {field_name}")
        
        # Check field types
        for field_name, expected_type in self.FIELD_TYPES.items():
            if field_name in record and record[field_name] is not None:
                value = record[field_name]
                actual_type = type(value).__name__
                
                if expected_type == "number" and not isinstance(value, (int, float)):
                    errors.append(f"Field {field_name} expected {expected_type}, got {actual_type}")
                elif expected_type == "integer" and not isinstance(value, int):
                    errors.append(f"Field {field_name} expected {expected_type}, got {actual_type}")
        
        # Check for obvious anomalies
        if "latitude" in record or "lat" in record:
            lat = record.get("latitude") or record.get("lat")
            if lat is not None:
                try:
                    lat_val = float(lat)
                    if not (-90 <= lat_val <= 90):
                        warnings.append(f"Latitude {lat_val} out of range [-90, 90]")
                except (ValueError, TypeError):
                    errors.append(f"Invalid latitude value: {lat}")
        
        if "longitude" in record or "lon" in record:
            lon = record.get("longitude") or record.get("lon") or record.get("lng")
            if lon is not None:
                try:
                    lon_val = float(lon)
                    if not (-180 <= lon_val <= 180):
                        warnings.append(f"Longitude {lon_val} out of range [-180, 180]")
                except (ValueError, TypeError):
                    errors.append(f"Invalid longitude value: {lon}")
        
        # Check JSON structure validity
        for key, value in record.items():
            if isinstance(value, str):
                # Check for malformed JSON strings
                if value.startswith("{") or value.startswith("["):
                    try:
                        json.loads(value)
                    except json.JSONDecodeError:
                        errors.append(f"Field {key} contains malformed JSON")
        
        is_valid = len(errors) == 0
        
        return ValidationResult(
            record=record,
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
        )
    
    def validate_records(
        self,
        records: Iterable[dict[str, Any]],
        quarantine_invalid: bool = True,
    ) -> BatchValidationResult:
        """Validate a batch of records.
        
        Args:
            records: Records to validate
            quarantine_invalid: Whether to quarantine invalid records
            
        Returns:
            BatchValidationResult with summary
        """
        result = BatchValidationResult(
            source_id=self.source_id,
            run_id=self.run_id,
        )
        
        valid_records = []
        invalid_records = []
        
        for record in records:
            result.total_records += 1
            
            validation = self.validate_record(record)
            
            if validation.is_valid:
                result.valid_records += 1
                valid_records.append(record)
            else:
                result.invalid_records += 1
                result.validation_errors.append({
                    "record_key": self._get_record_key(record),
                    "errors": validation.errors,
                    "warnings": validation.warnings,
                    "record_sample": self._get_error_sample(record),
                })
                invalid_records.append(record)
            
            result.warnings_count += len(validation.warnings)
        
        # Save validation results
        self._save_validation_result(result)
        
        # Quarantine invalid records
        if quarantine_invalid and invalid_records:
            result.quarantined_path = self._quarantine_records(invalid_records)
        
        return result
    
    def validate_streaming(
        self,
        records: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """Streaming validation that yields only valid records.
        
        Args:
            records: Input records
            
        Yields:
            Only valid records
        """
        for record in records:
            validation = self.validate_record(record)
            
            if validation.is_valid:
                yield record
            else:
                self._validation_errors.append({
                    "record_key": self._get_record_key(record),
                    "errors": validation.errors,
                    "record_sample": self._get_error_sample(record),
                })
    
    def _get_record_key(self, record: dict[str, Any]) -> str:
        """Generate a key for the record for error tracking."""
        # Try to find a natural key
        for key in ["id", "record_id", "index", "accident_index"]:
            if key in record and record[key]:
                return f"{key}={record[key]}"
        
        # Fall back to hashing
        import hashlib
        record_str = json.dumps(record, sort_keys=True, default=str)
        return hashlib.md5(record_str.encode()).hexdigest()[:8]
    
    def _get_error_sample(self, record: dict[str, Any]) -> dict[str, Any]:
        """Get a sample of the record for error reporting."""
        sample = {}
        for key in list(record.keys())[:5]:
            value = record[key]
            if isinstance(value, str) and len(value) > 100:
                sample[key] = value[:100] + "..."
            else:
                sample[key] = value
        return sample
    
    def _save_validation_result(self, result: BatchValidationResult) -> Path:
        """Save validation result to disk."""
        path = self.validation_dir / self.source_id / f"{self.run_id}.validation.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = result.to_dict()
        data["validation_errors"] = result.validation_errors[:100]  # Limit stored errors
        
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return path
    
    def _quarantine_records(self, records: list[dict[str, Any]]) -> Path | None:
        """Quarantine invalid records."""
        if not records:
            return None
        
        try:
            return quarantine_records(
                dataset=self.source_id,
                records=records,
                reason="bronze_schema_validation_failed",
                batch_id=self.run_id,
                run_id=self.run_id,
                layer="bronze",
            )
        except Exception as e:
            logger.error(f"Failed to quarantine records: {e}")
            return None
    
    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics for this run."""
        return {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "total_errors": len(self._validation_errors),
            "errors": self._validation_errors[:10],  # Last 10 errors
        }


def validate_bronze_records(
    source_id: str,
    run_id: str,
    records: list[dict[str, Any]],
    quarantine: bool = True,
) -> BatchValidationResult:
    """Convenience function for Bronze validation.
    
    Args:
        source_id: Source identifier
        run_id: Run identifier
        records: Records to validate
        quarantine: Whether to quarantine invalid records
        
    Returns:
        BatchValidationResult
    """
    validator = BronzeSchemaValidator(source_id, run_id)
    return validator.validate_records(records, quarantine_invalid=quarantine)


def validate_streaming_bronze(
    source_id: str,
    run_id: str,
    records: Iterable[dict[str, Any]],
) -> Iterable[dict[str, Any]]:
    """Streaming validation for Bronze layer.
    
    Args:
        source_id: Source identifier
        run_id: Run identifier
        records: Input records
        
    Yields:
        Only valid records
    """
    validator = BronzeSchemaValidator(source_id, run_id)
    return validator.validate_streaming(records)
