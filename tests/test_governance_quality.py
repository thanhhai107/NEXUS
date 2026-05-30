"""Tests for governance.quality modules."""

from __future__ import annotations

import pytest

from governance.quality.rules import (
    QualityRule,
    ValidationResult,
    validate_record,
    validate_batch,
)


class TestQualityRules:
    """Tests for quality validation rules."""

    def test_validate_record_not_null(self):
        """Test not_null validation."""
        rule = QualityRule(
            name="id_required",
            column="id",
            rule_type="not_null",
        )
        
        # Valid record
        result = validate_record({"id": 1}, rule)
        assert result.passed
        
        # Invalid record
        result = validate_record({"id": None}, rule)
        assert not result.passed

    def test_validate_record_in_range(self):
        """Test in_range validation."""
        rule = QualityRule(
            name="age_range",
            column="age",
            rule_type="in_range",
            params={"min": 0, "max": 120},
        )
        
        # Valid
        result = validate_record({"age": 30}, rule)
        assert result.passed
        
        # Too low
        result = validate_record({"age": -1}, rule)
        assert not result.passed
        
        # Too high
        result = validate_record({"age": 150}, rule)
        assert not result.passed

    def test_validate_record_regex(self):
        """Test regex validation."""
        rule = QualityRule(
            name="email_format",
            column="email",
            rule_type="regex",
            params={"pattern": r"^[\w\.-]+@[\w\.-]+\.\w+$"},
        )
        
        # Valid
        result = validate_record({"email": "test@example.com"}, rule)
        assert result.passed
        
        # Invalid
        result = validate_record({"email": "invalid-email"}, rule)
        assert not result.passed

    def test_validate_batch(self):
        """Test batch validation."""
        rules = [
            QualityRule(name="id_req", column="id", rule_type="not_null"),
            QualityRule(name="name_req", column="name", rule_type="not_null"),
        ]
        
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2},  # Missing name
            {"name": "Bob"},  # Missing id
        ]
        
        results, failed_records = validate_batch(records, rules)
        
        assert len(results) == 6  # 3 records x 2 rules
        assert len(failed_records) == 2  # Record 2 and 3

    def test_validation_result_severity(self):
        """Test validation result severity."""
        rule = QualityRule(
            name="test",
            column="field",
            rule_type="not_null",
            severity="warning",
        )
        
        result = validate_record({"field": None}, rule)
        assert result.severity == "warning"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test creating validation result."""
        result = ValidationResult(
            rule_name="test_rule",
            column="test_column",
            passed=True,
            message="Field is valid",
            severity="error",
        )
        
        assert result.rule_name == "test_rule"
        assert result.column == "test_column"
        assert result.passed
        assert result.message == "Field is valid"
        assert result.severity == "error"
        assert result.failed_count == 0
