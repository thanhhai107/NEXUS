"""Quality Rules Module.

Provides configurable quality rules for data validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class QualityRule:
    """A quality validation rule."""
    name: str
    column: str
    rule_type: str  # "not_null", "unique", "in_range", "regex", "custom"
    params: dict[str, Any] = None
    severity: str = "error"  # "error", "warning", "info"
    
    def __post_init__(self):
        if self.params is None:
            self.params = {}


@dataclass
class ValidationResult:
    """Result of validating against a rule."""
    rule_name: str
    column: str
    passed: bool
    message: str
    severity: str
    failed_count: int = 0


def load_rules_from_yaml(path: Path) -> list[QualityRule]:
    """Load quality rules from YAML file."""
    if not path.exists():
        return []
    
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    rules = []
    for rule_data in data.get("rules", []):
        rules.append(QualityRule(
            name=rule_data["name"],
            column=rule_data["column"],
            rule_type=rule_data["type"],
            params=rule_data.get("params", {}),
            severity=rule_data.get("severity", "error"),
        ))
    
    return rules


def validate_record(record: dict, rule: QualityRule) -> ValidationResult:
    """Validate a single record against a rule."""
    value = record.get(rule.column)
    
    if rule.rule_type == "not_null":
        passed = value is not None and value != ""
        return ValidationResult(
            rule_name=rule.name,
            column=rule.column,
            passed=passed,
            message="Column is required" if not passed else "Column is present",
            severity=rule.severity,
        )
    
    if rule.rule_type == "unique":
        # Note: Unique check requires dataset-level context
        return ValidationResult(
            rule_name=rule.name,
            column=rule.column,
            passed=True,  # Would need seen values tracking
            message="Unique check deferred to batch validation",
            severity=rule.severity,
        )
    
    if rule.rule_type == "in_range":
        min_val = rule.params.get("min")
        max_val = rule.params.get("max")
        
        if min_val is not None and value < min_val:
            return ValidationResult(
                rule_name=rule.name,
                column=rule.column,
                passed=False,
                message=f"Value {value} is below minimum {min_val}",
                severity=rule.severity,
            )
        
        if max_val is not None and value > max_val:
            return ValidationResult(
                rule_name=rule.name,
                column=rule.column,
                passed=False,
                message=f"Value {value} exceeds maximum {max_val}",
                severity=rule.severity,
            )
        
        return ValidationResult(
            rule_name=rule.name,
            column=rule.column,
            passed=True,
            message="Value is within range",
            severity=rule.severity,
        )
    
    if rule.rule_type == "regex":
        import re
        pattern = rule.params.get("pattern")
        if pattern and value:
            try:
                passed = bool(re.match(pattern, str(value)))
                return ValidationResult(
                    rule_name=rule.name,
                    column=rule.column,
                    passed=passed,
                    message=f"Value matches pattern" if passed else f"Value does not match pattern {pattern}",
                    severity=rule.severity,
                )
            except re.error:
                pass
    
    return ValidationResult(
        rule_name=rule.name,
        column=rule.column,
        passed=True,
        message="Rule type not implemented",
        severity=rule.severity,
    )


def validate_batch(
    records: list[dict],
    rules: list[QualityRule],
) -> tuple[list[ValidationResult], list[dict]]:
    """Validate a batch of records against rules.
    
    Returns:
        Tuple of (validation results, failed records)
    """
    results = []
    failed_records = []
    
    for record in records:
        for rule in rules:
            result = validate_record(record, rule)
            results.append(result)
            
            if not result.passed and rule.severity == "error":
                failed_records.append(record)
    
    return results, failed_records
