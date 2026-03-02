"""
Weather API client for Open-Meteo with batching and retry logic.

Fetches current weather data for multiple cities per request
to stay within API rate limits.
"""

from __future__ import annotations

import time
from datetime import datetime

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.cities import City, get_city_batches
from src.config.settings import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WeatherAPIClient:
    """Client to fetch weather data from Open-Meteo API with batching and retries."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.base_url = self.settings.API_BASE_URL
        self.timeout = self.settings.API_TIMEOUT_SECONDS
        self.rate_limit_delay = self.settings.RATE_LIMIT_DELAY_SECONDS

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _fetch_batch(
        self, cities: list[City]
    ) -> dict:
        """Fetch weather data for a batch of cities in a single API call.

        Args:
            cities: List of City objects to fetch data for.

        Returns:
            Dict mapping city names to their raw weather data.

        Raises:
            httpx.HTTPStatusError: If the API returns a non-2xx status code.
            httpx.TimeoutException: If the request times out.
        """
        latitudes = ",".join(str(c.latitude) for c in cities)
        longitudes = ",".join(str(c.longitude) for c in cities)

        params = {
            "latitude": latitudes,
            "longitude": longitudes,
            "current_weather": "true",
        }

        start_time = time.monotonic()

        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(self.base_url, params=params)
            response.raise_for_status()

        elapsed_ms = (time.monotonic() - start_time) * 1000
        data = response.json()

        # Open-Meteo returns a list when multiple locations are queried,
        # but a single object when only one location is queried.
        if isinstance(data, dict) and "current_weather" in data:
            # Single city response — wrap in a list
            data = [data]

        results = {}
        for i, city in enumerate(cities):
            if i < len(data):
                results[city.name] = {
                    "city": city,
                    "raw_response": data[i],
                    "api_response_time_ms": elapsed_ms,
                    "fetched_at": datetime.utcnow().isoformat(),
                }

        logger.info(
            f"Fetched batch of {len(cities)} cities in {elapsed_ms:.1f}ms",
            batch_size=len(cities),
            response_time_ms=elapsed_ms,
        )

        return results

    def fetch_all_cities(self) -> list[dict]:
        """Fetch weather data for all 500 cities using batched requests.

        Returns:
            List of dicts with raw weather data per city.
        """
        all_results = []
        batches = get_city_batches(self.settings.BATCH_SIZE)

        logger.info(
            f"Starting extraction: {len(batches)} batches of "
            f"{self.settings.BATCH_SIZE} cities",
            total_cities=500,
            batch_count=len(batches),
        )

        for batch_idx, batch in enumerate(batches):
            try:
                batch_results = self._fetch_batch(batch)
                all_results.extend(batch_results.values())
                logger.info(
                    f"Batch {batch_idx + 1}/{len(batches)} complete",
                    batch_index=batch_idx + 1,
                    cities_fetched=len(batch_results),
                )
            except Exception as e:
                logger.error(
                    f"Batch {batch_idx + 1}/{len(batches)} failed: {e}",
                    batch_index=batch_idx + 1,
                    error=str(e),
                )
                # Record failed cities with error info
                for city in batch:
                    all_results.append({
                        "city": city,
                        "raw_response": None,
                        "error": str(e),
                        "fetched_at": datetime.utcnow().isoformat(),
                    })

            # Rate limiting between batches
            if batch_idx < len(batches) - 1:
                time.sleep(self.rate_limit_delay)

        successful = sum(1 for r in all_results if r.get("raw_response") is not None)
        failed = len(all_results) - successful
        logger.info(
            f"Extraction complete: {successful} succeeded, {failed} failed",
            successful=successful,
            failed=failed,
        )

        return all_results
