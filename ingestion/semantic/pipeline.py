"""
Semantic Annotation Pipeline.

Orchestrates the complete semantic annotation workflow:
1. Schema inference (done externally)
2. Diff check (detect changes)
3. Template matching (free, fast)
4. LLM annotation (if needed)
5. Cache update
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ingestion.downloaders.schema_inference import InferredSchema

# Default settings
DEFAULT_CACHE_DIR = "semantic_cache"
DEFAULT_LLM_MODEL = "amazon.nova-pro-v1:0"
DEFAULT_MIN_NEW_FIELDS = 3
DEFAULT_REANNOTATE_THRESHOLD = 10


@dataclass
class AnnotationResult:
    """Result of semantic annotation pipeline."""
    
    source_id: str
    new_fields_count: int = 0
    llm_calls: int = 0
    template_annotations_count: int = 0
    annotations: dict[str, dict] = field(default_factory=dict)
    schema_hash: str = ""
    from_cache: bool = False
    error: str | None = None
    
    @property
    def total_annotations(self) -> int:
        """Total number of annotations."""
        return len(self.annotations)
    
    @property
    def needs_human_review(self) -> bool:
        """Check if needs human review."""
        return self.llm_calls > 0


class SemanticAnnotationPipeline:
    """
    Orchestrates semantic annotation workflow.
    
    Flow:
    1. Receive inferred schema from schema_inference.py
    2. Check diff vs cached annotations
    3. Template matching for common patterns
    4. LLM annotation for new fields
    5. Update cache
    
    Usage:
        pipeline = SemanticAnnotationPipeline(
            cache_dir="semantic_cache",
            llm_model="qwen2.5:0.5b"
        )
        
        result = pipeline.process(
            source_id="tfl_arrivals",
            source_key="tfl_arrivals",
            inferred_schema=schema,
            docs_url="https://api.tfl.gov.uk/"
        )
        
        if result.needs_human_review:
            # Show for review
    """
    
    def __init__(
        self,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        llm_model: str = DEFAULT_LLM_MODEL,
        llm_region: str = "us-east-1",
        min_new_fields: int = DEFAULT_MIN_NEW_FIELDS,
        reannotate_threshold: int = DEFAULT_REANNOTATE_THRESHOLD,
        llm_timeout: int = 180,
    ):
        """
        Initialize pipeline.
        
        Args:
            cache_dir: Path to semantic cache directory
            llm_model: Bedrock model ID
            llm_region: AWS region
            min_new_fields: Minimum new fields to trigger LLM
            reannotate_threshold: Threshold for full re-annotation
            llm_timeout: LLM request timeout in seconds
        """
        self.cache_dir = Path(cache_dir)
        self.llm_model = llm_model
        self.llm_region = llm_region
        self.min_new_fields = min_new_fields
        self.reannotate_threshold = reannotate_threshold
        self.llm_timeout = llm_timeout
        
        # Lazy import to avoid circular dependency
        from ingestion.semantic.diff_detector import SchemaDiffDetector
        from ingestion.semantic.template_annotator import TemplateAnnotator
        from ingestion.semantic.cache import SemanticCache
        from ingestion.semantic.llm_annotator import BedrockAnnotator
        from ingestion.semantic.fetch_docs import fetch_api_docs
        
        self.diff_detector = SchemaDiffDetector(cache_dir)
        self.template_annotator = TemplateAnnotator()
        self.cache = SemanticCache(cache_dir)
        self.llm_annotator = BedrockAnnotator(
            model=llm_model,
            region=llm_region,
            timeout=llm_timeout,
        )
        self.fetch_docs = fetch_api_docs
    
    def process(
        self,
        source_id: str,
        source_key: str,
        inferred_schema: InferredSchema,
        docs_url: str | None = None,
        domain: str = "unknown",
    ) -> AnnotationResult:
        """
        Process a source through the annotation pipeline.
        
        Args:
            source_id: Source identifier
            source_key: Source key (for fetching docs)
            inferred_schema: Schema from schema_inference.py
            docs_url: URL of API documentation
            domain: Domain name for context
        
        Returns:
            AnnotationResult with annotations and metadata
        """
        result = AnnotationResult(source_id=source_id)
        
        try:
            # Step 1: Diff check
            diff = self.diff_detector.detect_changes(source_id, inferred_schema)
            result.schema_hash = diff.schema_hash
            
            # Step 2: Check if we need to do anything
            if not diff.has_changes and not diff.is_new_source:
                # Schema stable - use cached
                cached = self.cache.get(source_id)
                if cached:
                    result.annotations = cached.annotations
                    result.from_cache = True
                    result.template_annotations_count = len(result.annotations)
                    return result
            
            # Step 3: Template matching for all fields
            all_fields = list(inferred_schema.fields.keys())
            template_annotations = self.template_annotator.annotate_batch(
                inferred_schema.fields
            )
            result.template_annotations_count = len(template_annotations)
            
            # Step 4: Get cached annotations
            cached = self.cache.get(source_id)
            cached_annotations = cached.annotations if cached else {}
            
            # Step 5: Determine fields needing LLM
            fields_needing_llm = self._get_fields_needing_llm(
                all_fields=all_fields,
                template_annotations=template_annotations,
                cached_annotations=cached_annotations,
                diff=diff,
            )
            
            # Step 6: Fetch docs if URL provided (and non-empty)
            api_docs = None
            if docs_url and docs_url.strip():
                api_docs = self.fetch_docs(docs_url)
                # Only use docs if fetch was successful
                if not api_docs or len(api_docs.strip()) < 50:
                    api_docs = None
            
            # Step 7: Load samples
            samples = self._load_samples(inferred_schema)
            
            # Step 8: LLM annotation if fields need it
            llm_annotations = {}
            
            if fields_needing_llm:
                # Build fields dict for LLM
                fields_for_llm = {
                    name: inferred_schema.fields[name]
                    for name in fields_needing_llm
                    if name in inferred_schema.fields
                }
                
                if fields_for_llm:
                    try:
                        # Always try LLM if fields need annotation
                        # Even without docs, LLM should return empty for unknown fields
                        llm_annotations = self.llm_annotator.annotate(
                            source_id=source_id,
                            fields=fields_for_llm,
                            api_docs=api_docs,
                            samples=samples,
                            domain=domain,
                        )
                        result.llm_calls = 1
                        result.new_fields_count = len(fields_needing_llm)
                        
                        # Count how many fields got annotated
                        annotated_count = len(llm_annotations)
                        empty_count = len(fields_needing_llm) - annotated_count
                        
                        if empty_count > 0:
                            print(f"[DEBUG] LLM returned empty for {empty_count}/{len(fields_needing_llm)} fields")
                            
                    except Exception as e:
                        result.error = f"LLM annotation failed: {e}"
                        print(f"Warning: {result.error}")
            
            # Step 9: Merge all annotations
            result.annotations = self._merge_annotations(
                template_annotations=template_annotations,
                llm_annotations=llm_annotations,
                cached_annotations=cached_annotations,
                all_fields=all_fields,
            )
            
            # Step 10: Update cache
            cache_version = self.cache.set(
                source_id=source_id,
                annotations=result.annotations,
                schema_hash=result.schema_hash,
                annotated_by="llm" if llm_annotations else "template",
            )
            cache_path = self.cache.cache_dir / source_id / cache_version / "annotations.json"
            print(f"[DEBUG] Semantic cache: source={source_id} version={cache_version} fields={len(result.annotations)} path={cache_path}")
            
            return result
            
        except Exception as e:
            result.error = str(e)
            print(f"Error in annotation pipeline for {source_id}: {e}")
            return result
    
    def _get_fields_needing_llm(
        self,
        all_fields: list[str],
        template_annotations: dict[str, dict],
        cached_annotations: dict[str, dict],
        diff,
    ) -> list[str]:
        """
        Determine which fields need LLM annotation.
        
        Args:
            all_fields: All field names
            template_annotations: Template matches
            cached_annotations: Cached annotations
            diff: DiffResult
        
        Returns:
            List of field names needing LLM
        """
        fields_needing_llm = []
        
        for field_name in all_fields:
            # Skip if template matched
            if field_name in template_annotations:
                continue
            
            # Skip if already cached
            if field_name in cached_annotations:
                continue
            
            # New field needs LLM
            if field_name in diff.new_fields:
                fields_needing_llm.append(field_name)
        
        # Check if we should re-annotate entire source
        if diff.should_reannotate:
            fields_needing_llm = [
                f for f in all_fields
                if f not in template_annotations
            ]
        
        return fields_needing_llm
    
    def _load_samples(self, schema: InferredSchema) -> list[dict]:
        """
        Extract sample data from schema.
        
        Args:
            schema: InferredSchema
        
        Returns:
            List of sample records
        """
        samples = []
        
        for field_name, field_schema in schema.fields.items():
            if hasattr(field_schema, "sample_values") and field_schema.sample_values:
                for sample in field_schema.sample_values[:5]:
                    if isinstance(sample, dict):
                        samples.append(sample)
                        break
        
        return samples[:5] if samples else []
    
    def _merge_annotations(
        self,
        template_annotations: dict[str, dict],
        llm_annotations: dict[str, dict],
        cached_annotations: dict[str, dict],
        all_fields: list[str],
    ) -> dict[str, dict]:
        """
        Merge annotations from different sources.
        
        Priority: LLM > Template > Cached
        
        Fields without any annotation will have an empty annotation to indicate
        they need manual review or better documentation.
        
        Args:
            template_annotations: From template matching
            llm_annotations: From LLM
            cached_annotations: From cache
            all_fields: All field names
        
        Returns:
            Merged annotations dict
        """
        merged = {}
        
        for field_name in all_fields:
            # LLM has highest priority
            if field_name in llm_annotations:
                merged[field_name] = llm_annotations[field_name]
            # Template second
            elif field_name in template_annotations:
                merged[field_name] = template_annotations[field_name]
            # Cached last
            elif field_name in cached_annotations:
                merged[field_name] = cached_annotations[field_name]
            # No annotation available - mark as empty
            else:
                merged[field_name] = {
                    "description": "",
                    "role": "",
                    "confidence": 0.0,
                    "source": "none",
                    "needs_review": True,
                }
        
        return merged
    
    def check_llm_health(self) -> dict[str, Any]:
        """
        Check LLM health status.
        
        Returns:
            Health status dict
        """
        return self.llm_annotator.check_health()
    
    def get_cache_status(self) -> dict[str, Any]:
        """
        Get cache status.
        
        Returns:
            Status dict
        """
        return self.cache.get_status()


def run_pipeline(
    source_id: str,
    source_key: str,
    schema_path: Path | str,
    docs_url: str | None = None,
    cache_dir: Path | str = DEFAULT_CACHE_DIR,
    llm_model: str = DEFAULT_LLM_MODEL,
) -> AnnotationResult:
    """
    Convenience function to run pipeline.
    
    Args:
        source_id: Source identifier
        source_key: Source key
        schema_path: Path to inferred schema JSON
        docs_url: API documentation URL
        cache_dir: Cache directory
        llm_model: LLM model name
    
    Returns:
        AnnotationResult
    """
    from ingestion.downloaders.schema_inference import InferredSchema
    
    # Load schema
    schema_data = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    
    # Reconstruct InferredSchema (simplified)
    schema = InferredSchema(
        source_id=source_id,
        source_key=source_key,
        run_id=schema_data.get("x-inference", {}).get("run_id"),
        inferred_at=schema_data.get("x-inference", {}).get("inferred_at", ""),
        record_count=schema_data.get("x-inference", {}).get("record_count", 0),
    )
    
    # Load fields
    for field_name, field_data in schema_data.get("properties", {}).items():
        from ingestion.downloaders.schema_inference import FieldSchema
        
        schema.fields[field_name] = FieldSchema(
            name=field_name,
            inferred_type=field_data.get("type", "unknown"),
            nullable=isinstance(field_data.get("type"), list),
            pattern=field_data.get("format"),
        )
    
    # Run pipeline
    pipeline = SemanticAnnotationPipeline(
        cache_dir=cache_dir,
        llm_model=llm_model,
        llm_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    
    return pipeline.process(
        source_id=source_id,
        source_key=source_key,
        inferred_schema=schema,
        docs_url=docs_url,
    )
