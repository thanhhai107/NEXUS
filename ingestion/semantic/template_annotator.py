"""
Template-based Semantic Annotator.

Handles common field patterns without requiring LLM calls.
This is free (no API cost) and fast.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from governance.schema.inference import FieldSchema


# Template patterns for common field types
FIELD_TEMPLATES = [
    # Identifiers
    {
        "pattern": r"^id$",
        "role": "primary_key",
        "description_template": "{field} is the unique identifier for the record",
    },
    {
        "pattern": r"^(id|{source}_id|[a-z]+_id)$",
        "role": "identifier",
        "description_template": "{field} is an identifier for {entity}",
    },
    {
        "pattern": r"^.*_id$",
        "role": "foreign_key",
        "description_template": "{field} is a reference to {referenced_entity}",
    },
    {
        "pattern": r"^uuid$|^guid$",
        "role": "identifier",
        "subtype": "uuid",
        "description_template": "{field} is a globally unique identifier",
    },
    
    # Temporal fields
    {
        "pattern": r"^.*_at$",
        "role": "temporal",
        "description_template": "{field} represents the timestamp of {event_type}",
    },
    {
        "pattern": r"^.*_time$",
        "role": "temporal",
        "description_template": "{field} is the time of {event_type}",
    },
    {
        "pattern": r"^created_at$|^created$",
        "role": "audit_time",
        "description_template": "{field} is when the record was created",
    },
    {
        "pattern": r"^updated_at$|^modified_at$|^modified$",
        "role": "audit_time",
        "description_template": "{field} is when the record was last modified",
    },
    {
        "pattern": r"^deleted_at$",
        "role": "audit_time",
        "description_template": "{field} is when the record was deleted",
    },
    {
        "pattern": r"^.*_datetime$|^.*_timestamp$",
        "role": "temporal",
        "description_template": "{field} is a datetime value",
    },
    
    # Geospatial
    {
        "pattern": r"^.*_lat$|^.*_latitude$|^lat$|^latitude$",
        "role": "geospatial",
        "subtype": "latitude",
        "unit": "degrees",
        "description_template": "{field} is the latitude coordinate",
    },
    {
        "pattern": r"^.*_lon$|^.*_lng$|^.*_longitude$|^lon$|^lng$|^longitude$",
        "role": "geospatial",
        "subtype": "longitude",
        "unit": "degrees",
        "description_template": "{field} is the longitude coordinate",
    },
    {
        "pattern": r"^.*_alt$|^.*_altitude$|^.*_elevation$",
        "role": "geospatial",
        "subtype": "altitude",
        "unit": "meters",
        "description_template": "{field} is the altitude/elevation",
    },
    {
        "pattern": r"^.*_bbox$|^.*_bounding_box$",
        "role": "geospatial",
        "subtype": "bounding_box",
        "description_template": "{field} defines a bounding box region",
    },
    
    # Descriptive
    {
        "pattern": r"^.*_name$|^name$",
        "role": "descriptive",
        "subtype": "name",
        "description_template": "{field} is the name of {entity}",
    },
    {
        "pattern": r"^.*_label$|^.*_title$|^label$|^title$",
        "role": "descriptive",
        "subtype": "label",
        "description_template": "{field} is the label/title of {entity}",
    },
    {
        "pattern": r"^.*_description$|^description$|^desc$",
        "role": "descriptive",
        "subtype": "description",
        "description_template": "{field} provides a description of {entity}",
    },
    {
        "pattern": r"^.*_address$|^address$",
        "role": "descriptive",
        "subtype": "address",
        "description_template": "{field} is the address of {entity}",
    },
    
    # Measures (numeric aggregates)
    {
        "pattern": r"^.*_count$",
        "role": "measure",
        "subtype": "count",
        "description_template": "{field} is the count of {entity}",
    },
    {
        "pattern": r"^.*_total$|^.*_sum$",
        "role": "measure",
        "subtype": "aggregate",
        "description_template": "{field} is the total/sum of {entity}",
    },
    {
        "pattern": r"^.*_avg$|^.*_average$|^.*_mean$",
        "role": "measure",
        "subtype": "average",
        "description_template": "{field} is the average of {entity}",
    },
    {
        "pattern": r"^.*_max$|^maximum$",
        "role": "measure",
        "subtype": "maximum",
        "description_template": "{field} is the maximum value of {entity}",
    },
    {
        "pattern": r"^.*_min$|^minimum$",
        "role": "measure",
        "subtype": "minimum",
        "description_template": "{field} is the minimum value of {entity}",
    },
    
    # Monetary
    {
        "pattern": r"^.*_amount$",
        "role": "measure",
        "subtype": "monetary",
        "description_template": "{field} is the monetary amount of {entity}",
    },
    {
        "pattern": r"^.*_price$|^.*_cost$",
        "role": "measure",
        "subtype": "price",
        "description_template": "{field} is the price/cost of {entity}",
    },
    {
        "pattern": r"^.*_value$",
        "role": "measure",
        "subtype": "value",
        "description_template": "{field} is the value of {entity}",
    },
    {
        "pattern": r"^.*_fee$|^.*_charge$",
        "role": "measure",
        "subtype": "fee",
        "description_template": "{field} is the fee/charge for {entity}",
    },
    {
        "pattern": r"^.*_budget$|^.*_revenue$|^.*_income$",
        "role": "measure",
        "subtype": "financial",
        "description_template": "{field} is the financial amount for {entity}",
    },
    
    # Rates and ratios
    {
        "pattern": r"^.*_rate$",
        "role": "measure",
        "subtype": "rate",
        "description_template": "{field} is a rate value for {entity}",
    },
    {
        "pattern": r"^.*_ratio$|^.*_percentage$|^.*_percent$",
        "role": "measure",
        "subtype": "ratio",
        "description_template": "{field} is a ratio/percentage for {entity}",
    },
    
    # Status
    {
        "pattern": r"^.*_status$",
        "role": "status",
        "description_template": "{field} indicates the status of {entity}",
    },
    {
        "pattern": r"^.*_state$",
        "role": "status",
        "description_template": "{field} represents the state of {entity}",
    },
    {
        "pattern": r"^.*_condition$",
        "role": "status",
        "description_template": "{field} indicates the condition of {entity}",
    },
    
    # Flags (boolean)
    {
        "pattern": r"^is_.*$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates whether {condition}",
    },
    {
        "pattern": r"^has_.*$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates whether {entity} has {attribute}",
    },
    {
        "pattern": r"^was_.*$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates whether {action} occurred",
    },
    {
        "pattern": r"^did_.*$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates whether {entity} did {action}",
    },
    {
        "pattern": r"^can_.*$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates capability of {entity}",
    },
    {
        "pattern": r"^enable.*$|^disabled$",
        "role": "flag",
        "subtype": "boolean",
        "description_template": "{field} indicates enabled/disabled status",
    },
    
    # Metadata
    {
        "pattern": r"^version$",
        "role": "metadata",
        "description_template": "{field} is the version identifier",
    },
    {
        "pattern": r"^.*_version$",
        "role": "metadata",
        "description_template": "{field} is the version of {entity}",
    },
    {
        "pattern": r"^source$",
        "role": "metadata",
        "description_template": "{field} indicates the source of the data",
    },
    {
        "pattern": r"^.*_source$",
        "role": "metadata",
        "description_template": "{field} is the source of {entity}",
    },
    {
        "pattern": r"^.*_type$|^type$|^category$",
        "role": "dimension",
        "subtype": "categorical",
        "description_template": "{field} is the type/category of {entity}",
    },
    
    # Codes and IDs
    {
        "pattern": r"^.*_code$",
        "role": "dimension",
        "subtype": "code",
        "description_template": "{field} is a code identifier for {entity}",
    },
    {
        "pattern": r"^.*_key$",
        "role": "identifier",
        "description_template": "{field} is a key for {entity}",
    },
    
    # Location
    {
        "pattern": r"^.*_location$|^location$",
        "role": "geospatial",
        "subtype": "location",
        "description_template": "{field} is the location of {entity}",
    },
    {
        "pattern": r"^.*_region$|^region$",
        "role": "dimension",
        "subtype": "region",
        "description_template": "{field} is the region of {entity}",
    },
    {
        "pattern": r"^.*_country$|^country$",
        "role": "dimension",
        "subtype": "country",
        "description_template": "{field} is the country of {entity}",
    },
    {
        "pattern": r"^.*_city$|^city$",
        "role": "dimension",
        "subtype": "city",
        "description_template": "{field} is the city of {entity}",
    },
    
    # Time periods
    {
        "pattern": r"^.*_year$",
        "role": "dimension",
        "subtype": "year",
        "description_template": "{field} is the year of {entity}",
    },
    {
        "pattern": r"^.*_month$",
        "role": "dimension",
        "subtype": "month",
        "description_template": "{field} is the month of {entity}",
    },
    {
        "pattern": r"^.*_day$",
        "role": "dimension",
        "subtype": "day",
        "description_template": "{field} is the day of {entity}",
    },
    {
        "pattern": r"^.*_hour$",
        "role": "dimension",
        "subtype": "hour",
        "description_template": "{field} is the hour of {entity}",
    },
    {
        "pattern": r"^.*_week$",
        "role": "dimension",
        "subtype": "week",
        "description_template": "{field} is the week of {entity}",
    },
]


@dataclass
class TemplateAnnotation:
    """Result of template-based annotation."""
    
    description: str
    role: str
    unit: str | None = None
    subtype: str | None = None
    confidence: float = 0.7  # Template has lower confidence than LLM
    source: str = "template"


class TemplateAnnotator:
    """
    Template-based semantic annotator.
    
    Handles common field patterns without LLM calls.
    This is fast and free.
    
    Usage:
        annotator = TemplateAnnotator()
        annotation = annotator.annotate("created_at", field_schema)
        
        if annotation:
            # Use template annotation
        else:
            # Need LLM
    """
    
    def __init__(self):
        """Initialize template annotator."""
        # Compile regex patterns for efficiency
        self._compiled_templates = []
        for template in FIELD_TEMPLATES:
            pattern = template["pattern"]
            compiled = re.compile(pattern, re.IGNORECASE)
            self._compiled_templates.append((compiled, template))
    
    def annotate(
        self,
        field_name: str,
        field_schema: FieldSchema | None = None,
    ) -> dict | None:
        """
        Try to annotate a field using templates.
        
        Args:
            field_name: Name of the field
            field_schema: Optional FieldSchema for additional context
        
        Returns:
            Annotation dict if template matches, None otherwise
        """
        for compiled_pattern, template in self._compiled_templates:
            if compiled_pattern.match(field_name):
                return self._build_annotation(field_name, template, field_schema)
        
        return None
    
    def annotate_batch(
        self,
        fields: dict[str, FieldSchema],
    ) -> dict[str, dict]:
        """
        Annotate multiple fields using templates.
        
        Args:
            fields: Dict of field_name -> FieldSchema
        
        Returns:
            Dict of field_name -> annotation (only for matched fields)
        """
        annotations = {}
        
        for field_name, field_schema in fields.items():
            annotation = self.annotate(field_name, field_schema)
            if annotation:
                annotations[field_name] = annotation
        
        return annotations
    
    def _build_annotation(
        self,
        field_name: str,
        template: dict,
        field_schema: FieldSchema | None,
    ) -> dict:
        """
        Build annotation dict from template.
        
        Args:
            field_name: Name of the field
            template: Template dict
            field_schema: Optional FieldSchema
        
        Returns:
            Annotation dict
        """
        # Render description template
        description_template = template.get("description_template", "{field}")
        description = description_template.format(
            field=field_name,
            entity=field_name.replace("_", " "),
            event_type=field_name.replace("_", " "),
            condition=field_name.replace("is_", "").replace("_", " "),
            action=field_name.replace("did_", "").replace("_", " "),
            attribute=field_name.replace("has_", "").replace("_", " "),
        )
        
        return {
            "description": description,
            "role": template.get("role", "dimension"),
            "unit": template.get("unit"),
            "subtype": template.get("subtype"),
            "confidence": template.get("confidence", 0.7),
            "source": "template",
        }
