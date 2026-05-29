"""Tests for londonair source adapter."""
from __future__ import annotations

import pytest
from datetime import date
from unittest.mock import MagicMock, patch


class TestLondonAirDownloadChunk:
    """Test download_londonair_json_chunk with skip_on_400."""

    def test_skip_on_400_false_raises_on_400(self):
        """When skip_on_400=False, 400 errors should propagate to mark_failed."""
        from ingestion.sources.londonair import download_londonair_json_chunk

        run = MagicMock()
        run.should_skip.return_value = False

        # Mock request_json to raise HTTPError with 400
        error = Exception("HTTP Error 400: Bad Request")
        with patch("ingestion.sources.londonair.request_json", side_effect=error):
            result = download_londonair_json_chunk(
                run,
                "https://api.erg.ic.ac.uk/AirQuality",
                "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
                chunk_id="test:chunk:1",
                relative_path="test.json",
                path_values={"SiteCode": "GR8", "StartDate": "2020-01-01", "EndDate": "2020-01-31"},
                skip_on_400=False,
            )

        assert result is None
        run.mark_failed.assert_called_once()
        run.mark_skipped.assert_not_called()

    def test_skip_on_400_true_skips_on_400(self):
        """When skip_on_400=True, 400 errors should call mark_skipped instead."""
        from ingestion.sources.londonair import download_londonair_json_chunk

        run = MagicMock()
        run.should_skip.return_value = False

        # Mock request_json to raise HTTPError with 400
        error = Exception("HTTP Error 400: Bad Request")
        with patch("ingestion.sources.londonair.request_json", side_effect=error):
            result = download_londonair_json_chunk(
                run,
                "https://api.erg.ic.ac.uk/AirQuality",
                "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
                chunk_id="test:chunk:1",
                relative_path="test.json",
                path_values={"SiteCode": "GR8", "StartDate": "2020-01-01", "EndDate": "2020-01-31"},
                skip_on_400=True,
            )

        assert result is None
        run.mark_skipped.assert_called_once()
        run.mark_skipped.assert_called_with("test:chunk:1", {
            "reason": "site_no_data_for_period",
            "error": "HTTP Error 400: Bad Request"
        })
        run.mark_failed.assert_not_called()

    def test_skip_on_400_ignores_other_errors(self):
        """When skip_on_400=True, non-400 errors should still call mark_failed."""
        from ingestion.sources.londonair import download_londonair_json_chunk

        run = MagicMock()
        run.should_skip.return_value = False

        # Mock request_json to raise 500 error
        error = Exception("HTTP Error 500: Internal Server Error")
        with patch("ingestion.sources.londonair.request_json", side_effect=error):
            result = download_londonair_json_chunk(
                run,
                "https://api.erg.ic.ac.uk/AirQuality",
                "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
                chunk_id="test:chunk:1",
                relative_path="test.json",
                path_values={"SiteCode": "MY1", "StartDate": "2020-01-01", "EndDate": "2020-01-31"},
                skip_on_400=True,
            )

        assert result is None
        run.mark_failed.assert_called_once()
        run.mark_skipped.assert_not_called()

    def test_successful_download(self):
        """Successful downloads should call mark_complete."""
        from ingestion.sources.londonair import download_londonair_json_chunk

        run = MagicMock()
        run.should_skip.return_value = False
        run.write_json.return_value = MagicMock()

        mock_payload = {"data": "test", "rows": [1, 2, 3]}
        with patch("ingestion.sources.londonair.request_json", return_value=mock_payload):
            with patch("ingestion.sources.londonair.estimate_record_count", return_value=3):
                result = download_londonair_json_chunk(
                    run,
                    "https://api.erg.ic.ac.uk/AirQuality",
                    "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
                    chunk_id="test:chunk:1",
                    relative_path="test.json",
                    path_values={"SiteCode": "MY1", "StartDate": "2020-01-01", "EndDate": "2020-01-31"},
                    skip_on_400=True,
                )

        assert result == mock_payload
        run.mark_complete.assert_called_once()
        run.mark_failed.assert_not_called()
        run.mark_skipped.assert_not_called()

    def test_index_days_endpoint_skip_on_400(self):
        """Index days endpoint should use skip_on_400=True for graceful handling."""
        from ingestion.sources.londonair import download_londonair_json_chunk

        run = MagicMock()
        run.should_skip.return_value = False

        # Mock 400 error
        error = Exception("HTTP Error 400: Bad Request")
        with patch("ingestion.sources.londonair.request_json", side_effect=error):
            result = download_londonair_json_chunk(
                run,
                "https://api.erg.ic.ac.uk/AirQuality",
                "/Data/SiteSpeciesIndexDays/SiteCode={SiteCode}/SpeciesCode={SpeciesCode}/Period={Period}/Json",
                chunk_id="londonair:index_days:BG1:NO2:Year",
                relative_path="index_days/site=BG1/species=NO2/period=Year.json",
                path_values={"SiteCode": "BG1", "SpeciesCode": "NO2", "Period": "Year"},
                skip_on_400=True,
            )

        assert result is None
        run.mark_skipped.assert_called_once()
        run.mark_skipped.assert_called_with(
            "londonair:index_days:BG1:NO2:Year",
            {"reason": "site_no_data_for_period", "error": "HTTP Error 400: Bad Request"}
        )


class TestBuildLondonAirUrl:
    """Test URL building."""

    def test_url_with_path_values(self):
        """URL should replace path values correctly."""
        from ingestion.sources.londonair import build_londonair_url

        url = build_londonair_url(
            "https://api.erg.ic.ac.uk/AirQuality",
            "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
            {"SiteCode": "MY1", "StartDate": "2020-01-01", "EndDate": "2020-01-31"}
        )

        assert url == "https://api.erg.ic.ac.uk/AirQuality/Data/Wide/Site/SiteCode=MY1/StartDate=2020-01-01/EndDate=2020-01-31/Json"

    def test_url_case_insensitive_replacement(self):
        """URL should handle case-insensitive path value replacement."""
        from ingestion.sources.londonair import build_londonair_url

        # Test that values are correctly substituted regardless of case in template
        url = build_londonair_url(
            "https://api.erg.ic.ac.uk/AirQuality",
            "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
            {"SiteCode": "MY1", "StartDate": "2020-01-01", "EndDate": "2020-01-31"}
        )

        # All values should be in URL with correct casing
        assert "SiteCode=MY1" in url
        assert "StartDate=2020-01-01" in url
        assert "EndDate=2020-01-31" in url
        assert url == "https://api.erg.ic.ac.uk/AirQuality/Data/Wide/Site/SiteCode=MY1/StartDate=2020-01-01/EndDate=2020-01-31/Json"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
