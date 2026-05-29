"""Tests for openmeteo source adapter."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestDownloadOpenMeteo:
    """Test download_openmeteo function defensive handling."""

    def test_missing_both_urls_raises_source_failure(self):
        """When both URLs are missing, raise SourceFailure."""
        from ingestion.base.core import SourceFailure
        from ingestion.sources.openmeteo import download_openmeteo

        context = MagicMock()
        context.mode = {"core_start": "2025-01-01", "core_end": "2025-01-02"}
        context.config = {"openmeteo": {}}
        context.spatial_scope = {}

        run = MagicMock()

        with pytest.raises(SourceFailure) as exc_info:
            download_openmeteo(run, context)

        assert "not configured" in str(exc_info.value)

    def test_no_boroughs_raises_source_failure(self):
        """When no boroughs are available, raise SourceFailure."""
        from ingestion.base.core import SourceFailure
        from ingestion.sources.openmeteo import download_openmeteo

        context = MagicMock()
        context.mode = {"core_start": "2025-01-01", "core_end": "2025-01-02"}
        context.config = {
            "openmeteo": {
                "air_quality_url": "https://api.open-meteo.com/v1/air-quality",
            }
        }
        context.spatial_scope = {}

        run = MagicMock()

        with patch("ingestion.sources.openmeteo.selected_boroughs", return_value=[]):
            with pytest.raises(SourceFailure) as exc_info:
                download_openmeteo(run, context)

        assert "boroughs" in str(exc_info.value).lower()

    def test_services_list_built_correctly_with_both_urls(self):
        """Services list should include both air_quality and weather when both URLs present."""
        from ingestion.sources.openmeteo import download_openmeteo

        context = MagicMock()
        context.mode = {"core_start": "2025-01-01", "core_end": "2025-01-02"}
        context.config = {
            "openmeteo": {
                "air_quality_url": "https://api.open-meteo.com/v1/air-quality",
                "weather_url": "https://api.open-meteo.com/v1/forecast",
            }
        }
        context.spatial_scope = {}

        run = MagicMock()
        run.should_skip.return_value = False

        mock_boroughs = [{"name": "Westminster", "latitude": 51.4975, "longitude": -0.1357}]
        mock_payload = {"hourly": {"time": ["2025-01-01T00:00"]}}

        call_count = 0

        def mock_request_json(run_arg, url, *, params=None, headers=None, timeout=None, delay_seconds=None):
            nonlocal call_count
            call_count += 1
            return mock_payload

        with patch("ingestion.sources.openmeteo.selected_boroughs", return_value=mock_boroughs):
            with patch("ingestion.sources.openmeteo.request_json", side_effect=mock_request_json):
                with patch("ingestion.sources.openmeteo.estimate_record_count", return_value=1):
                    download_openmeteo(run, context)

                    # Both services should be called for 1 borough
                    assert call_count == 2

    def test_hourly_excluded_when_empty(self):
        """Hourly param should not be in params when empty list."""
        from ingestion.sources.openmeteo import download_openmeteo

        context = MagicMock()
        context.mode = {"core_start": "2025-01-01", "core_end": "2025-01-02"}
        context.config = {
            "openmeteo": {
                "air_quality_url": "https://api.open-meteo.com/v1/air-quality",
                "air_quality_hourly": [],  # Empty
            }
        }
        context.spatial_scope = {}

        run = MagicMock()
        run.should_skip.return_value = False

        mock_boroughs = [{"name": "Westminster", "latitude": 51.4975, "longitude": -0.1357}]
        mock_payload = {"hourly": {"time": ["2025-01-01T00:00"]}}

        captured_params = {}

        def mock_request_json(run_arg, url, *, params=None, headers=None, timeout=None, delay_seconds=None):
            captured_params.update(params or {})
            return mock_payload

        with patch("ingestion.sources.openmeteo.selected_boroughs", return_value=mock_boroughs):
            with patch("ingestion.sources.openmeteo.request_json", side_effect=mock_request_json):
                with patch("ingestion.sources.openmeteo.estimate_record_count", return_value=1):
                    download_openmeteo(run, context)

                    # hourly should not be in params when empty
                    assert "hourly" not in captured_params


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
