"""Schema Module.

Provides schema inference and versioning.
"""

from governance.schema.inference import (
    SchemaInference,
    InferredSchema,
    FieldSchema,
    infer_and_save,
)
from governance.schema.versioning import (
    SchemaVersion,
    SchemaRegistry,
    register_schema,
    get_latest_schema,
    compare_schemas,
)

__all__ = [
    "SchemaInference",
    "InferredSchema",
    "FieldSchema",
    "infer_and_save",
    "SchemaVersion",
    "SchemaRegistry",
    "register_schema",
    "get_latest_schema",
    "compare_schemas",
]
