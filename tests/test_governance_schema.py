"""Tests for governance.schema modules."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from governance.schema.inference import (
    SchemaInference,
    InferredSchema,
    FieldSchema,
)


class TestSchemaInference:
    """Tests for schema inference."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.inference = SchemaInference()

    def test_infer_from_records(self):
        """Test inferring schema from records."""
        records = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        assert schema.source_id == "trade"
        assert schema.record_count == 3
        assert "id" in schema.fields
        assert "name" in schema.fields
        assert "age" in schema.fields
        assert schema.fields["id"].inferred_type == "integer"
        assert schema.fields["name"].inferred_type == "string"
        assert schema.fields["age"].inferred_type == "integer"

    def test_infer_with_null_values(self):
        """Test inferring schema with null values."""
        records = [
            {"id": 1, "name": None, "age": 30},
            {"id": 2, "name": "Bob", "age": None},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        assert schema.fields["name"].nullable
        assert schema.fields["age"].nullable

    def test_infer_nested_object(self):
        """Test inferring schema with nested objects."""
        records = [
            {"id": 1, "address": {"city": "London", "zip": "SW1A"}},
            {"id": 2, "address": {"city": "Paris", "zip": "75001"}},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        # Nested fields should be flattened
        assert "address_city" in schema.fields
        assert "address_zip" in schema.fields

    def test_to_dict_format(self):
        """Test schema to_dict format."""
        records = [
            {"id": 1, "name": "Alice"},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        schema_dict = schema.to_dict()
        
        assert "$schema" in schema_dict
        assert "properties" in schema_dict
        assert "id" in schema_dict["properties"]
        assert "name" in schema_dict["properties"]

    def test_to_summary_format(self):
        """Test schema to_summary format."""
        records = [
            {"id": 1, "name": "Alice"},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        summary = schema.to_summary()
        
        assert summary["source_id"] == "trade"
        assert summary["field_count"] == 2
        assert "fields" in summary

    def test_save_and_load_schema(self):
        """Test saving and loading schema."""
        records = [
            {"id": 1, "name": "Alice"},
        ]
        
        schema = self.inference.infer_from_records(
            records,
            source_id="trade",
            source_key="test_key",
        )
        
        schema_path = Path(self.temp_dir) / "test.schema.json"
        schema.save(schema_path)
        
        assert schema_path.exists()
        
        # Load and verify
        loaded_data = schema_path.read_text()
        loaded = json.loads(loaded_data)
        assert "properties" in loaded


class TestFieldSchema:
    """Tests for FieldSchema dataclass."""

    def test_null_ratio_calculation(self):
        """Test null ratio calculation."""
        field = FieldSchema(
            name="test_field",
            inferred_type="string",
            total_count=100,
            null_count=25,
        )
        
        assert field.null_ratio == 0.25

    def test_to_dict_output(self):
        """Test field to_dict output."""
        field = FieldSchema(
            name="test_field",
            inferred_type="integer",
            nullable=True,
            total_count=50,
            null_count=10,
            sample_values=[1, 2, 3],
        )
        
        field_dict = field.to_dict()
        
        assert field_dict["name"] == "test_field"
        assert field_dict["type"] == "integer"
        assert field_dict["nullable"]
        assert field_dict["null_ratio"] == 0.2
        assert field_dict["sample_values"] == [1, 2, 3]
