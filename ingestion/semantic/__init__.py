"""
Semantic Annotation Module.

Provides LLM-assisted semantic metadata enrichment for data fields.

Usage:
    from ingestion.semantic import SemanticAnnotationPipeline

    pipeline = SemanticAnnotationPipeline(cache_dir="semantic_cache")
    result = pipeline.process(
        source_id="tpcds_store_sales",
        source_key="tpcds_store_sales",
        inferred_schema=schema,
        docs_url="https://api.tfl.gov.uk/"
    )
"""

from ingestion.semantic.diff_detector import SchemaDiffDetector, DiffResult
from ingestion.semantic.template_annotator import TemplateAnnotator
from ingestion.semantic.cache import SemanticCache, CachedAnnotations
from ingestion.semantic.fetch_docs import fetch_api_docs
from ingestion.semantic.llm_annotator import BedrockAnnotator
from ingestion.semantic.pipeline import SemanticAnnotationPipeline, AnnotationResult

__all__ = [
    # Classes
    "SchemaDiffDetector",
    "DiffResult",
    "TemplateAnnotator",
    "SemanticCache",
    "CachedAnnotations",
    "BedrockAnnotator",
    "SemanticAnnotationPipeline",
    "AnnotationResult",
    # Functions
    "fetch_api_docs",
]

__version__ = "1.0.0"
