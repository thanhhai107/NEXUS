"""
London Data Source Adapters - Backward Compatibility Module.

This module re-exports from ingestion.sources for backward compatibility.
The actual source adapters are now in ingestion/sources/.
"""

from ingestion.sources.londonair import download_londonair
from ingestion.sources.openaq import download_openaq
from ingestion.sources.openmeteo import download_openmeteo
from ingestion.sources.openmeteo_historical_weather import download_openmeteo_historical_weather
from ingestion.sources.ukair import download_ukair_air_quality_archive
from ingestion.sources.waqi import download_waqi
from ingestion.sources.openweather import download_openweather
from ingestion.sources.tfl import download_tfl, download_tfl_arrivals, download_tfl_line_status
from ingestion.sources.stats19 import download_stats19
from ingestion.sources.naptan import download_naptan
from ingestion.sources.dft import download_dft
from ingestion.sources.london_journeys import download_london_journeys
from ingestion.sources.ncei import download_ncei

__all__ = [
    "download_londonair",
    "download_openaq",
    "download_openmeteo",
    "download_openmeteo_historical_weather",
    "download_ukair_air_quality_archive",
    "download_waqi",
    "download_openweather",
    "download_tfl",
    "download_tfl_arrivals",
    "download_tfl_line_status",
    "download_stats19",
    "download_naptan",
    "download_dft",
    "download_london_journeys",
    "download_ncei",
]
