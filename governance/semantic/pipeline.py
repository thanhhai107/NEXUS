"""Semantic Annotation Pipeline.

Extracted from ingestion/semantic/pipeline.py for governance service.
Orchestrates the complete semantic annotation workflow.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from governance.schema.inference import InferredSchema


DEFAULT_CACHE_DIR = "runtime/governance/semantic"
DEFAULT_LLM_MODEL = "qwen2.5:0.5b"
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
            cache_dir="governance/semantic",
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
        llm_base_url: str = "http://localhost:11434",
        min_new_fields: int = DEFAULT_MIN_NEW_FIELDS,
        reannotate_threshold: int = DEFAULT_REANNOTATE_THRESHOLD,
        llm_timeout: int = 180,
    ):
        """Initialize pipeline."""
        from governance.semantic.cache import SemanticCache
        from governance.semantic.diff_detector import SchemaDiffDetector
        from governance.semantic.template_annotator import TemplateAnnotator
        from governance.semantic.llm_annotator import OllamaAnnotator
        from governance.semantic.fetch_docs import fetch_api_docs
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.llm_model = llm_model
        self.llm_base_url = llm_base_url
        self.min_new_fields = min_new_fields
        self.reannotate_threshold = reannotate_threshold
        self.llm_timeout = llm_timeout
        
        self.diff_detector = SchemaDiffDetector(cache_dir)
        self.template_annotator = TemplateAnnotator()
        self.cache = SemanticCache(cache_dir)
        self.llm_annotator = OllamaAnnotator(
            model=llm_model,
            base_url=llm_base_url,
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
        """Process a source through the annotation pipeline."""
        result = AnnotationResult(source_id=source_id)
        
        try:
            # Step 1: Diff check
            diff = self.diff_detector.detect_changes(source_id, inferred_schema)
            result.schema_hash = diff.schema_hash
            
            # Step 2: Check if we need to do anything
            if not diff.has_changes and not diff.is_new_source:
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
            
            # Step 6: Fetch docs if URL provided
            api_docs = None
            if docs_url and docs_url.strip():
                api_docs = self.fetch_docs(docs_url)
                if not api_docs or len(api_docs.strip()) < 50:
                    api_docs = None
            
            # Step 7: Load samples
            samples = self._load_samples(inferred_schema)
            
            # Step 8: LLM annotation if fields need it
            llm_annotations = {}
            
            if fields_needing_llm:
                fields_for_llm = {
                    name: inferred_schema.fields[name]
                    for name in fields_needing_llm
                    if name in inferred_schema.fields
                }
                
                if fields_for_llm:
                    try:
                        llm_annotations = self.llm_annotator.annotate(
                            source_id=source_id,
                            fields=fields_for_llm,
                            api_docs=api_docs,
                            samples=samples,
                            domain=domain,
                        )
                        result.llm_calls = 1
                        result.new_fields_count = len(fields_needing_llm)
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
        """Determine which fields need LLM annotation."""
        fields_needing_llm = []
        
        for field_name in all_fields:
            if field_name in template_annotations:
                continue
            if field_name in cached_annotations:
                continue
            if field_name in diff.new_fields:
                fields_needing_llm.append(field_name)
        
        if diff.should_reannotate:
            fields_needing_llm = [
                f for f in all_fields
                if f not in template_annotations
            ]
        
        return fields_needing_llm
    
    def _load_samples(self, schema: InferredSchema) -> list[dict]:
        """Extract sample data from schema."""
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
        """Merge annotations from different sources."""
        merged = {}
        
        for field_name in all_fields:
            if field_name in llm_annotations:
                merged[field_name] = llm_annotations[field_name]
            elif field_name in template_annotations:
                merged[field_name] = template_annotations[field_name]
            elif field_name in cached_annotations:
                merged[field_name] = cached_annotations[field_name]
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
        """Check LLM health status."""
        return self.llm_annotator.check_health()
    
    def get_cache_status(self) -> dict[str, Any]:
        """Get cache status."""
        return self.cache.get_status()
