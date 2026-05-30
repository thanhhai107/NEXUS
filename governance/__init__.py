"""Governance Module.

Provides semantic annotation, quality validation, schema management, and data governance.

Structure:
    governance/
        ├── schema/              # Schema inference và versioning
        ├── catalog/            # Dataset registry
        ├── semantic/           # Semantic annotation
        ├── quality/            # Quality validation
        └── config/             # Governance configuration
"""

from governance.schema.inference import SchemaInference, InferredSchema
from governance.schema.versioning import SchemaRegistry, register_schema, get_latest_schema
from governance.catalog.dataset_registry import DatasetRegistry, register_dataset, get_dataset
from governance.semantic.pipeline import SemanticAnnotationPipeline, AnnotationResult
from governance.quality.rules import QualityRule, validate_record
from governance.quality.checks import QualityResult

__all__ = [
    # Schema
    "SchemaInference",
    "InferredSchema",
    "SchemaRegistry",
    "register_schema",
    "get_latest_schema",
    # Catalog
    "DatasetRegistry",
    "register_dataset",
    "get_dataset",
    # Semantic
    "SemanticAnnotationPipeline",
    "AnnotationResult",
    # Quality
    "QualityRule",
    "QualityResult",
    "validate_record",
]
