"""
Unit tests for the weather API extract layer.

Tests batch splitting, API response parsing, and error handling.
All API calls are mocked — no real HTTP requests are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.config.cities import City, get_city_batches
from src.extract.weather_api import WeatherAPIClient


class TestCityBatches:
    """Test city batch splitting logic."""

    def test_batch_size_default(self):
        batches = get_city_batches(batch_size=50)
        assert len(batches) == 10  # 500 cities / 50 = 10 batches
        assert len(batches[0]) == 50

    def test_batch_size_custom(self):
        batches = get_city_batches(batch_size=100)
        assert len(batches) == 5

    def test_all_cities_included(self):
        batches = get_city_batches(batch_size=50)
        total_cities = sum(len(b) for b in batches)
        assert total_cities == 500


class TestWeatherAPIClient:
    """Tests for the WeatherAPIClient."""

    @patch("src.extract.weather_api.httpx.Client")
    def test_fetch_batch_success(
        self, mock_client_cls, sample_cities, sample_batch_api_response
    ):
        """Test successful batch fetch with mocked HTTP response."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_batch_api_response
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        client = WeatherAPIClient()
        results = client._fetch_batch(sample_cities)

        assert len(results) == 3
        assert "Delhi" in results
        assert "Mumbai" in results
        assert "Bangalore" in results

        # Verify the raw response is attached
        assert results["Delhi"]["raw_response"]["current_weather"]["temperature"] == 30.0

    @patch("src.extract.weather_api.httpx.Client")
    def test_fetch_batch_single_city(
        self, mock_client_cls, sample_city, sample_api_response
    ):
        """Test single-city fetch (API returns dict instead of list)."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_api_response  # single dict
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        client = WeatherAPIClient()
        results = client._fetch_batch([sample_city])

        assert len(results) == 1
        assert "TestCity" in results

    @patch("src.extract.weather_api.httpx.Client")
    def test_fetch_all_cities_handles_errors(self, mock_client_cls, sample_cities):
        """Test that fetch_all_cities records errors for failed batches."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = Exception("Connection timeout")
        mock_client_cls.return_value = mock_client

        client = WeatherAPIClient()

        # Patch get_city_batches to return just one small batch
        with patch("src.extract.weather_api.get_city_batches") as mock_batches:
            mock_batches.return_value = [sample_cities]
            results = client.fetch_all_cities()

        # Should have error records for each city in the batch
        assert len(results) == 3
        for result in results:
            assert result.get("raw_response") is None
            assert "error" in result
