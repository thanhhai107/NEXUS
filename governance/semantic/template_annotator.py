"""Template-based Semantic Annotator.

Fast annotation using predefined templates for common field patterns.
"""

from __future__ import annotations

from typing import Any


# Common field patterns and their semantic annotations
TEMPLATE_ANNOTATIONS: dict[str, dict[str, Any]] = {
    # IDs
    "id": {"role": "identifier", "description": "Unique identifier", "confidence": 1.0},
    "uuid": {"role": "identifier", "description": "Universally unique identifier", "confidence": 1.0},
    "_id": {"role": "identifier", "description": "Record identifier", "confidence": 1.0},
    
    # Timestamps
    "timestamp": {"role": "timestamp", "description": "Event timestamp", "confidence": 1.0},
    "created_at": {"role": "timestamp", "description": "Creation timestamp", "confidence": 1.0},
    "updated_at": {"role": "timestamp", "description": "Last update timestamp", "confidence": 1.0},
    "datetime": {"role": "timestamp", "description": "Date and time", "confidence": 0.9},
    "date": {"role": "date", "description": "Date value", "confidence": 0.9},
    "time": {"role": "time", "description": "Time value", "confidence": 0.9},
    
    # Location
    "lat": {"role": "latitude", "description": "Latitude coordinate", "confidence": 1.0},
    "latitude": {"role": "latitude", "description": "Latitude coordinate", "confidence": 1.0},
    "lon": {"role": "longitude", "description": "Longitude coordinate", "confidence": 1.0},
    "lng": {"role": "longitude", "description": "Longitude coordinate", "confidence": 1.0},
    "longitude": {"role": "longitude", "description": "Longitude coordinate", "confidence": 1.0},
    "location": {"role": "location", "description": "Geographic location", "confidence": 0.8},
    "address": {"role": "address", "description": "Street address", "confidence": 0.9},
    
    # Names
    "name": {"role": "name", "description": "Name", "confidence": 0.9},
    "title": {"role": "name", "description": "Title or name", "confidence": 0.9},
    "description": {"role": "description", "description": "Description", "confidence": 1.0},
    
    # Measures
    "value": {"role": "measurement", "description": "Measured value", "confidence": 0.8},
    "count": {"role": "count", "description": "Count", "confidence": 0.9},
    "total": {"role": "total", "description": "Total amount", "confidence": 0.9},
    "average": {"role": "average", "description": "Average value", "confidence": 0.9},
    "min": {"role": "minimum", "description": "Minimum value", "confidence": 0.9},
    "max": {"role": "maximum", "description": "Maximum value", "confidence": 0.9},
    
    # AQI specific
    "pm25": {"role": "measurement", "description": "PM2.5 particulate matter", "confidence": 1.0},
    "pm10": {"role": "measurement", "description": "PM10 particulate matter", "confidence": 1.0},
    "no2": {"role": "measurement", "description": "Nitrogen dioxide", "confidence": 1.0},
    "o3": {"role": "measurement", "description": "Ozone", "confidence": 1.0},
    "co": {"role": "measurement", "description": "Carbon monoxide", "confidence": 1.0},
    "so2": {"role": "measurement", "description": "Sulphur dioxide", "confidence": 1.0},
    "aqi": {"role": "index", "description": "Air Quality Index", "confidence": 1.0},
    
    # Transport specific
    "line_id": {"role": "identifier", "description": "Transport line identifier", "confidence": 1.0},
    "stop_id": {"role": "identifier", "description": "Stop/station identifier", "confidence": 1.0},
    "arrival": {"role": "event", "description": "Arrival event", "confidence": 0.9},
    "departure": {"role": "event", "description": "Departure event", "confidence": 0.9},
    "platform": {"role": "attribute", "description": "Platform number/name", "confidence": 0.9},
    "direction": {"role": "attribute", "description": "Direction of travel", "confidence": 0.8},
}


class TemplateAnnotator:
    """Annotates fields using predefined templates."""
    
    def __init__(self):
        self.templates = TEMPLATE_ANNOTATIONS
    
    def annotate(self, field_name: str) -> dict[str, Any] | None:
        """Annotate a single field using template matching."""
        field_lower = field_name.lower()
        
        # Exact match
        if field_lower in self.templates:
            return self.templates[field_lower].copy()
        
        # Partial match
        for pattern, annotation in self.templates.items():
            if pattern in field_lower or field_lower in pattern:
                return annotation.copy()
        
        return None
    
    def annotate_batch(
        self,
        fields: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Annotate multiple fields."""
        annotations = {}
        
        for field_name in fields.keys():
            annotation = self.annotate(field_name)
            if annotation:
                annotations[field_name] = annotation
        
        return annotations
