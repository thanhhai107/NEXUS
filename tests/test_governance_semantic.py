"""Tests for governance.semantic modules."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from governance.semantic.cache import SemanticCache, CachedAnnotations
from governance.semantic.template_annotator import TemplateAnnotator


class TestSemanticCache:
    """Tests for semantic annotation cache."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = SemanticCache(self.temp_dir)

    def test_set_and_get_annotations(self):
        """Test setting and getting annotations."""
        annotations = {
            "id": {"role": "identifier", "description": "Unique ID", "confidence": 1.0},
            "name": {"role": "name", "description": "Person name", "confidence": 0.9},
        }
        
        version = self.cache.set(
            source_id="test_source",
            annotations=annotations,
            schema_hash="abc123",
            annotated_by="llm",
        )
        
        assert version.startswith("v")
        
        cached = self.cache.get("test_source")
        assert cached is not None
        assert cached.annotations["id"]["role"] == "identifier"

    def test_get_nonexistent_source(self):
        """Test getting annotations for non-existent source."""
        result = self.cache.get("nonexistent_source")
        assert result is None

    def test_list_sources(self):
        """Test listing all sources with cached annotations."""
        annotations = {"field1": {"role": "test"}}
        
        self.cache.set("source1", annotations, "hash1")
        self.cache.set("source2", annotations, "hash2")
        
        sources = self.cache.list_sources()
        assert "source1" in sources
        assert "source2" in sources

    def test_approve_annotations(self):
        """Test approving annotations."""
        annotations = {"field1": {"role": "test"}}
        
        self.cache.set("test_source", annotations, "hash1")
        assert not self.cache.is_approved("test_source")
        
        self.cache.approve("test_source", approved_by="human")
        assert self.cache.is_approved("test_source")

    def test_get_status(self):
        """Test getting cache status."""
        annotations = {"field1": {"role": "test"}}
        
        self.cache.set("source1", annotations, "hash1")
        self.cache.approve("source1")
        
        status = self.cache.get_status()
        assert "source1" in status
        assert status["source1"]["approved"]


class TestTemplateAnnotator:
    """Tests for template-based annotator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.annotator = TemplateAnnotator()

    def test_annotate_id_field(self):
        """Test annotating ID field."""
        result = self.annotator.annotate("id")
        
        assert result is not None
        assert result["role"] == "identifier"
        assert result["confidence"] == 1.0

    def test_annotate_timestamp_field(self):
        """Test annotating timestamp field."""
        result = self.annotator.annotate("created_at")
        
        assert result is not None
        assert result["role"] == "timestamp"
        assert result["confidence"] == 1.0

    def test_annotate_location_fields(self):
        """Test annotating location fields."""
        for field_name in ["latitude", "lat", "longitude", "lon", "lng"]:
            result = self.annotator.annotate(field_name)
            assert result is not None
            assert "latitude" in result["role"] or "longitude" in result["role"]

    def test_annotate_aqi_fields(self):
        """Test annotating AQI-specific fields."""
        for field_name in ["pm25", "pm10", "no2", "o3", "aqi"]:
            result = self.annotator.annotate(field_name)
            assert result is not None
            assert result["role"] == "measurement" or result["role"] == "index"

    def test_annotate_unknown_field(self):
        """Test annotating unknown field."""
        result = self.annotator.annotate("unknown_field_xyz")
        assert result is None

    def test_annotate_batch(self):
        """Test batch annotation."""
        fields = {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "unknown_field": {"type": "string"},
        }
        
        annotations = self.annotator.annotate_batch(fields)
        
        assert "id" in annotations
        assert "name" in annotations
        assert "unknown_field" not in annotations
