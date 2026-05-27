"""
London Downloader - Entry point for downloading London data sources.

This module provides the main CLI entry point for downloading data from
various London-specific data sources including air quality, weather, and transport.

Note: Core infrastructure has been moved to:
- ingestion/base/ - Core classes, HTTP client, contracts, utilities
- ingestion/sources/ - Source adapters

This module re-exports source adapters for backward compatibility.
"""

from ingestion.sources.londonair import download_londonair
from ingestion.sources.openaq import download_openaq
from ingestion.sources.openmeteo import download_openmeteo
from ingestion.sources.waqi import download_waqi
from ingestion.sources.openweather import download_openweather
from ingestion.sources.tfl import download_tfl
from ingestion.sources.stats19 import download_stats19
from ingestion.sources.naptan import download_naptan
from ingestion.sources.dft import download_dft
from ingestion.sources.london_journeys import download_london_journeys
from ingestion.sources.ncei import download_ncei

from ingestion.downloaders.london_downloader import (
    main,
    maybe_publish_raw_envelope,
    run_source,
    run_once,
    run_polling,
    SOURCE_REGISTRY,
)

__all__ = [
    # Source adapters
    "download_londonair",
    "download_openaq",
    "download_openmeteo",
    "download_waqi",
    "download_openweather",
    "download_tfl",
    "download_stats19",
    "download_naptan",
    "download_dft",
    "download_london_journeys",
    "download_ncei",
    # London downloader
    "main",
    "maybe_publish_raw_envelope",
    "run_source",
    "run_once",
    "run_polling",
    "SOURCE_REGISTRY",
]
