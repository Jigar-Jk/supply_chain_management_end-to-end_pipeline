"""
test_seed_api.py – Lightweight unit tests for seed_api cache logic.

These tests use unittest.mock to avoid real HTTP calls.
Run with:  python -m pytest tests/test_seed_api.py -v
      or:  python -m unittest tests.test_seed_api -v
"""

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Import the module under test
from data_generators.seed_api import (
    _make_route_cache_key,
    _make_weather_cache_key,
    _read_cache,
    _write_cache,
    fetch_route,
    fetch_weather,
    WEATHER_CACHE_TTL,
)


class TestRouteCacheKey(unittest.TestCase):
    """Cache key generation for routes should be deterministic."""

    def test_same_inputs_produce_same_key(self):
        key1 = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-hgv")
        key2 = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-hgv")
        self.assertEqual(key1, key2)

    def test_different_inputs_produce_different_keys(self):
        key1 = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-hgv")
        key2 = _make_route_cache_key([9.00, 50.00], [8.69, 49.42], "driving-hgv")
        self.assertNotEqual(key1, key2)

    def test_different_profiles_produce_different_keys(self):
        key1 = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-hgv")
        key2 = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-car")
        self.assertNotEqual(key1, key2)

    def test_key_is_hex_string(self):
        key = _make_route_cache_key([8.68, 49.41], [8.69, 49.42], "driving-hgv")
        self.assertEqual(len(key), 64)  # SHA-256 hex digest


class TestWeatherCacheKey(unittest.TestCase):
    """Cache key generation for weather should be deterministic."""

    def test_same_inputs_produce_same_key(self):
        key1 = _make_weather_cache_key(49.41, 8.68, "temperature_2m", 1, "UTC")
        key2 = _make_weather_cache_key(49.41, 8.68, "temperature_2m", 1, "UTC")
        self.assertEqual(key1, key2)

    def test_different_coords_produce_different_keys(self):
        key1 = _make_weather_cache_key(49.41, 8.68, "temperature_2m", 1, "UTC")
        key2 = _make_weather_cache_key(50.00, 9.00, "temperature_2m", 1, "UTC")
        self.assertNotEqual(key1, key2)


class TestCacheReadWrite(unittest.TestCase):
    """Test _read_cache and _write_cache with a temporary directory."""

    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.sample_data = {"routes": [{"summary": {"distance": 1234}}]}

    def test_write_then_read(self):
        _write_cache(self.tmp_dir, "test_key", self.sample_data)
        result = _read_cache(self.tmp_dir, "test_key")
        self.assertEqual(result, self.sample_data)

    def test_read_missing_returns_none(self):
        result = _read_cache(self.tmp_dir, "nonexistent")
        self.assertIsNone(result)

    def test_ttl_fresh_returns_data(self):
        _write_cache(self.tmp_dir, "fresh_key", self.sample_data)
        result = _read_cache(self.tmp_dir, "fresh_key", ttl=3600)
        self.assertEqual(result, self.sample_data)

    def test_ttl_expired_returns_none(self):
        _write_cache(self.tmp_dir, "old_key", self.sample_data)
        # Backdate the file modification time by 2 hours
        cache_file = self.tmp_dir / "old_key.json"
        old_time = time.time() - 7200
        os.utime(cache_file, (old_time, old_time))
        result = _read_cache(self.tmp_dir, "old_key", ttl=3600)
        self.assertIsNone(result)

    def test_no_ttl_never_expires(self):
        _write_cache(self.tmp_dir, "forever_key", self.sample_data)
        # Backdate by 30 days
        cache_file = self.tmp_dir / "forever_key.json"
        old_time = time.time() - (30 * 24 * 3600)
        os.utime(cache_file, (old_time, old_time))
        result = _read_cache(self.tmp_dir, "forever_key", ttl=None)
        self.assertEqual(result, self.sample_data)


class TestFetchRouteWithMock(unittest.TestCase):
    """Test fetch_route with mocked HTTP responses."""

    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.sample_response = {
            "routes": [{"summary": {"distance": 500, "duration": 60}}]
        }

    @patch("data_generators.seed_api.ROUTE_CACHE_DIR")
    @patch("data_generators.seed_api.requests.post")
    @patch.dict(os.environ, {"ORS_API_KEY": "test-key-not-real"})
    def test_fetches_and_caches_route(self, mock_post, mock_cache_dir):
        mock_cache_dir.__truediv__ = lambda self_, x: self.tmp_dir / x
        mock_cache_dir.mkdir = MagicMock()
        mock_cache_dir.exists = MagicMock(return_value=True)

        # Patch the cache dir used inside the function
        with patch("data_generators.seed_api.ROUTE_CACHE_DIR", self.tmp_dir):
            mock_resp = MagicMock()
            mock_resp.json.return_value = self.sample_response
            mock_resp.raise_for_status = MagicMock()
            mock_post.return_value = mock_resp

            result = fetch_route([8.68, 49.41], [8.69, 49.42])

            self.assertEqual(result, self.sample_response)
            mock_post.assert_called_once()

            # Second call should use cache, not make another request
            mock_post.reset_mock()
            result2 = fetch_route([8.68, 49.41], [8.69, 49.42])
            self.assertEqual(result2, self.sample_response)
            mock_post.assert_not_called()


class TestFetchWeatherWithMock(unittest.TestCase):
    """Test fetch_weather with mocked HTTP responses."""

    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.sample_response = {
            "latitude": 49.42,
            "longitude": 8.68,
            "hourly": {
                "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
                "temperature_2m": [5.0, 4.5],
            },
        }

    @patch("data_generators.seed_api.requests.get")
    def test_fetches_and_caches_weather(self, mock_get):
        with patch("data_generators.seed_api.WEATHER_CACHE_DIR", self.tmp_dir):
            mock_resp = MagicMock()
            mock_resp.json.return_value = self.sample_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = fetch_weather(49.42, 8.68)

            self.assertEqual(result, self.sample_response)
            mock_get.assert_called_once()

            # Second call should use cache
            mock_get.reset_mock()
            result2 = fetch_weather(49.42, 8.68)
            self.assertEqual(result2, self.sample_response)
            mock_get.assert_not_called()

    @patch("data_generators.seed_api.requests.get")
    def test_expired_cache_triggers_refresh(self, mock_get):
        with patch("data_generators.seed_api.WEATHER_CACHE_DIR", self.tmp_dir):
            # Pre-populate cache with an old file
            from data_generators.seed_api import _make_weather_cache_key, WEATHER_HOURLY_PARAMS
            key = _make_weather_cache_key(
                49.42, 8.68, WEATHER_HOURLY_PARAMS, 1, "UTC"
            )
            _write_cache(self.tmp_dir, key, self.sample_response)
            # Backdate the file
            cache_file = self.tmp_dir / f"{key}.json"
            old_time = time.time() - (WEATHER_CACHE_TTL + 100)
            os.utime(cache_file, (old_time, old_time))

            mock_resp = MagicMock()
            mock_resp.json.return_value = self.sample_response
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = fetch_weather(49.42, 8.68)
            self.assertEqual(result, self.sample_response)
            mock_get.assert_called_once()  # Should have made a fresh request


if __name__ == "__main__":
    unittest.main()
