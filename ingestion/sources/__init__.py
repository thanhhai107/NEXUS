"""
London Data Source Adapters.

Source adapters for downloading from specific London data sources.
Each module handles exactly one data source.
"""

# Air Quality
from ingestion.sources.londonair import download_londonair
from ingestion.sources.openaq import download_openaq
from ingestion.sources.openmeteo import download_openmeteo
from ingestion.sources.openmeteo_historical_weather import download_openmeteo_historical_weather
from ingestion.sources.ukair import download_ukair_air_quality_archive

# Real-time
from ingestion.sources.waqi import download_waqi
from ingestion.sources.openweather import download_openweather
from ingestion.sources.tfl import download_tfl, download_tfl_arrivals, download_tfl_line_status

# Transport
from ingestion.sources.stats19 import download_stats19
from ingestion.sources.naptan import download_naptan
from ingestion.sources.dft import download_dft
from ingestion.sources.london_journeys import download_london_journeys

# Climate
from ingestion.sources.ncei import download_ncei

__all__ = [
    # Air Quality
    "download_londonair",
    "download_openaq",
    "download_openmeteo",
    "download_openmeteo_historical_weather",
    "download_ukair_air_quality_archive",
    # Real-time
    "download_waqi",
    "download_openweather",
    "download_tfl",
    "download_tfl_arrivals",
    "download_tfl_line_status",
    # Transport
    "download_stats19",
    "download_naptan",
    "download_dft",
    "download_london_journeys",
    # Climate
    "download_ncei",
]
