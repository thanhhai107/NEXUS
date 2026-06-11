"""Schema Inference Module.

Extracted from ingestion/downloaders/schema_inference.py for governance service.
Provides schema inference and versioning for datasets.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import RUNTIME_DIR


SCHEMA_DIR = RUNTIME_DIR / "catalog" / "schemas"


PRIMITIVE_TYPES = {"string", "integer", "number", "boolean", "object", "array", "null"}


@dataclass
class FieldSchema:
    """Schema for a single field."""
    name: str
    inferred_type: str
    nullable: bool = False
    null_count: int = 0
    total_count: int = 0
    sample_values: list[Any] = field(default_factory=list)
    min_value: Any = None
    max_value: Any = None
    unique_count: int = 0
    pattern: str | None = None

    @property
    def null_ratio(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.inferred_type,
            "nullable": self.nullable,
            "null_ratio": round(self.null_ratio, 4),
            "unique_count": self.unique_count,
            "sample_values": self.sample_values[:5],
            "min_value": self.min_value,
            "max_value": self.max_value,
            "pattern": self.pattern,
        }


@dataclass
class InferredSchema:
    """Complete inferred schema for a dataset."""
    source_id: str
    source_key: str
    run_id: str | None
    inferred_at: str
    record_count: int = 0
    fields: dict[str, FieldSchema] = field(default_factory=dict)
    nested_paths: dict[str, list[str]] = field(default_factory=dict)
    version: str = "1.0"

    def add_field(self, name: str, value: Any) -> None:
        """Add or update a field with new value."""
        if name not in self.fields:
            self.fields[name] = FieldSchema(name=name, inferred_type="null")

        field_schema = self.fields[name]
        field_schema.total_count += 1

        if value is None:
            field_schema.null_count += 1
            field_schema.nullable = True
        else:
            value_type = self._infer_type(value)
            if field_schema.inferred_type == "null":
                field_schema.inferred_type = value_type
            elif field_schema.inferred_type != value_type:
                field_schema.inferred_type = "string"

            if len(field_schema.sample_values) < 5:
                field_schema.sample_values.append(value)

            if field_schema.inferred_type in ("integer", "number"):
                if field_schema.min_value is None or value < field_schema.min_value:
                    field_schema.min_value = value
                if field_schema.max_value is None or value > field_schema.max_value:
                    field_schema.max_value = value

            if field_schema.inferred_type == "string" and isinstance(value, str):
                pattern = self._detect_pattern(value)
                if pattern and field_schema.pattern is None:
                    field_schema.pattern = pattern

    def _infer_type(self, value: Any) -> str:
        """Infer type from a single value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        return "string"

    def _detect_pattern(self, value: str) -> str | None:
        """Detect common patterns in string values."""
        if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value):
            return "datetime"
        if re.match(r"\d{4}-\d{2}-\d{2}", value):
            return "date"
        if re.match(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", value, re.I):
            return "uuid"
        if "@" in value and "." in value.split("@")[-1]:
            return "email"
        if value.startswith(("http://", "https://")):
            return "url"
        if re.match(r"-?\d+\.\d+", value):
            return "coordinate"
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON Schema format."""
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": self.source_id,
            "type": "object",
            "x-inference": {
                "version": self.version,
                "source_key": self.source_key,
                "run_id": self.run_id,
                "inferred_at": self.inferred_at,
                "record_count": self.record_count,
            },
            "properties": {
                name: self._field_to_json_schema(field_schema)
                for name, field_schema in self.fields.items()
            },
            "required": [
                name for name, field in self.fields.items()
                if not field.nullable
            ] if self.fields else [],
        }

    def _field_to_json_schema(self, field_schema: FieldSchema) -> dict[str, Any]:
        """Convert field schema to JSON Schema."""
        output: dict[str, Any] = {"type": field_schema.inferred_type}

        if field_schema.nullable:
            output["type"] = [field_schema.inferred_type, "null"]

        if field_schema.min_value is not None:
            if field_schema.inferred_type == "integer":
                output["minimum"] = field_schema.min_value
            elif field_schema.inferred_type == "number":
                output["minimum"] = field_schema.min_value
                output["maximum"] = field_schema.max_value

        if field_schema.pattern:
            output["format"] = field_schema.pattern

        if field_schema.sample_values:
            output["x-sample"] = field_schema.sample_values[:3]

        return output

    def to_summary(self) -> dict[str, Any]:
        """Convert to summary format."""
        return {
            "source_id": self.source_id,
            "source_key": self.source_key,
            "run_id": self.run_id,
            "inferred_at": self.inferred_at,
            "record_count": self.record_count,
            "field_count": len(self.fields),
            "fields": {
                name: {
                    "type": f.inferred_type,
                    "nullable": f.nullable,
                    "null_ratio": round(f.null_ratio, 4),
                    "format": f.pattern,
                }
                for name, f in sorted(self.fields.items())
            },
        }

    def save(self, output_path: Path) -> None:
        """Save schema to JSON file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
            f.write("\n")

    def save_summary(self, output_path: Path) -> None:
        """Save schema summary to JSON file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(self.to_summary(), f, indent=2, ensure_ascii=False)
            f.write("\n")


class SchemaInference:
    """Main schema inference class."""

    def __init__(self, sample_size: int = 10000, flatten_separator: str = "_"):
        """Initialize schema inference.

        Args:
            sample_size: Maximum records to sample for inference.
            flatten_separator: Separator for flattened nested fields.
        """
        self.sample_size = sample_size
        self.flatten_separator = flatten_separator

    def infer_from_jsonl(
        self,
        file_path: Path,
        source_id: str,
        source_key: str,
        run_id: str | None = None,
    ) -> InferredSchema:
        """Infer schema from a JSONL file."""
        schema = InferredSchema(
            source_id=source_id,
            source_key=source_key,
            run_id=run_id,
            inferred_at=datetime.now(timezone.utc).isoformat(),
        )

        record_count = 0

        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                record_count += 1
                flat_fields = self._flatten_record(record)

                for field_name, field_value in flat_fields.items():
                    schema.add_field(field_name, field_value)

                if self.sample_size and record_count >= self.sample_size:
                    break

        schema.record_count = record_count
        return schema

    def _flatten_record(
        self,
        record: dict[str, Any],
        parent_key: str = "",
        sep: str = None,
    ) -> dict[str, Any]:
        """Flatten nested record to dot-separated keys."""
        if sep is None:
            sep = self.flatten_separator

        items: dict[str, Any] = {}

        if "payload" in record:
            record = record["payload"]

        for key, value in record.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.update(self._flatten_record(value, new_key, sep))
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                items[new_key] = value[0] if value else None
                items[f"{new_key}_count"] = len(value)
            else:
                items[new_key] = value

        return items

    def infer_from_records(
        self,
        records: list[dict[str, Any]],
        source_id: str,
        source_key: str,
        run_id: str | None = None,
    ) -> InferredSchema:
        """Infer schema from a list of records."""
        schema = InferredSchema(
            source_id=source_id,
            source_key=source_key,
            run_id=run_id,
            inferred_at=datetime.now(timezone.utc).isoformat(),
        )

        for record in records[: self.sample_size or len(records)]:
            flat_fields = self._flatten_record(record)
            for field_name, field_value in flat_fields.items():
                schema.add_field(field_name, field_value)

        schema.record_count = len(records)
        return schema


def infer_and_save(
    file_path: Path,
    output_dir: Path,
    source_id: str,
    source_key: str,
    run_id: str | None = None,
) -> InferredSchema:
    """Convenience function to infer schema and save to files."""
    inference = SchemaInference()
    schema = inference.infer_from_jsonl(file_path, source_id, source_key, run_id)

    schema_path = output_dir / f"{source_id}.schema.json"
    schema.save(schema_path)

    summary_path = output_dir / f"{source_id}.schema_summary.json"
    schema.save_summary(summary_path)

    return schema
