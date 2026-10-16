"""
test_normalize_api_data.py – Unit tests for normalize_api_data.

Tests normalization logic, error handling, deterministic ID generation,
and file output processing.

Run with:
    python -m unittest tests.test_normalize_api_data -v
"""

import json
import tempfile
import unittest
from pathlib import Path

from data_generators.normalize_api_data import (
    normalize_route,
    normalize_weather,
    process_route_cache,
    process_weather_cache,
)


class TestNormalizeRoute(unittest.TestCase):
    """Tests for normalize_route."""

    def setUp(self):
        self.sample_valid_ors = {
            "type": "FeatureCollection",
            "metadata": {
                "query": {
                    "coordinates": [[8.681495, 49.41461], [8.687872, 49.420318]],
                    "profile": "driving-hgv",
                }
            },
            "routes": [
                {
                    "summary": {
                        "distance": 1602.8,
                        "duration": 284.6,
                    }
                }
            ],
        }

    def test_valid_route_normalization(self):
        record = normalize_route(self.sample_valid_ors, "sample.json")
        self.assertIsNotNone(record)
        self.assertEqual(record["origin_longitude"], 8.681495)
        self.assertEqual(record["origin_latitude"], 49.41461)
        self.assertEqual(record["destination_longitude"], 8.687872)
        self.assertEqual(record["destination_latitude"], 49.420318)
        self.assertEqual(record["distance_meters"], 1602.8)
        self.assertEqual(record["duration_seconds"], 284.6)
        self.assertEqual(record["routing_profile"], "driving-hgv")
        self.assertEqual(record["source"], "openrouteservice")
        self.assertTrue(len(record["route_id"]) == 16)

    def test_missing_routes_raises_value_error(self):
        bad_data = {"metadata": self.sample_valid_ors["metadata"], "routes": []}
        with self.assertRaises(ValueError):
            normalize_route(bad_data, "bad.json")

    def test_missing_summary_raises_value_error(self):
        bad_data = {
            "metadata": self.sample_valid_ors["metadata"],
            "routes": [{}],
        }
        with self.assertRaises(ValueError):
            normalize_route(bad_data, "bad.json")

    def test_missing_coordinates_raises_value_error(self):
        bad_data = {
            "metadata": {"query": {}},
            "routes": [{"summary": {"distance": 100, "duration": 50}}],
        }
        with self.assertRaises(ValueError):
            normalize_route(bad_data, "bad.json")


class TestNormalizeWeather(unittest.TestCase):
    """Tests for normalize_weather."""

    def setUp(self):
        self.sample_valid_weather = {
            "latitude": 49.42275,
            "longitude": 8.69481,
            "hourly": {
                "time": ["2026-10-12T00:00", "2026-10-12T01:00"],
                "temperature_2m": [7.9, 8.1],
                "precipitation": [0.0, 0.2],
                "wind_speed_10m": [2.8, 3.5],
                "visibility": [25720.0, 24000.0],
            },
        }

    def test_valid_weather_normalization(self):
        records = normalize_weather(self.sample_valid_weather, "sample.json")
        self.assertEqual(len(records), 2)
        r0 = records[0]
        self.assertEqual(r0["latitude"], 49.42275)
        self.assertEqual(r0["longitude"], 8.69481)
        self.assertEqual(r0["forecast_time_utc"], "2026-10-12T00:00")
        self.assertEqual(r0["temperature_2m_c"], 7.9)
        self.assertEqual(r0["precipitation_mm"], 0.0)
        self.assertEqual(r0["wind_speed_10m_kmh"], 2.8)
        self.assertEqual(r0["visibility_m"], 25720.0)
        self.assertEqual(r0["source"], "open-meteo")
        self.assertEqual(len(r0["weather_record_id"]), 16)

    def test_missing_coordinates_raises_value_error(self):
        bad_data = {"hourly": self.sample_valid_weather["hourly"]}
        with self.assertRaises(ValueError):
            normalize_weather(bad_data, "bad.json")

    def test_missing_required_field_raises_value_error(self):
        bad_data = {
            "latitude": 49.42,
            "longitude": 8.69,
            "hourly": {
                "time": ["2026-10-12T00:00"],
                "temperature_2m": [7.9],
                # missing precipitation, wind_speed_10m, visibility
            },
        }
        with self.assertRaises(ValueError):
            normalize_weather(bad_data, "bad.json")

    def test_mismatched_array_lengths_raises_value_error(self):
        bad_data = {
            "latitude": 49.42,
            "longitude": 8.69,
            "hourly": {
                "time": ["2026-10-12T00:00", "2026-10-12T01:00"],
                "temperature_2m": [7.9],  # only 1 element
                "precipitation": [0.0, 0.2],
                "wind_speed_10m": [2.8, 3.5],
                "visibility": [25720.0, 24000.0],
            },
        }
        with self.assertRaises(ValueError):
            normalize_weather(bad_data, "bad.json")


class TestFileProcessing(unittest.TestCase):
    """Tests for process_route_cache and process_weather_cache."""

    def test_process_route_cache_skips_corrupted_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache_dir.mkdir()
            output_file = Path(tmpdir) / "output" / "routes.jsonl"

            # 1. Valid file
            valid_payload = {
                "metadata": {
                    "query": {
                        "coordinates": [[8.68, 49.41], [8.69, 49.42]],
                        "profile": "driving-hgv",
                    }
                },
                "routes": [{"summary": {"distance": 1000.0, "duration": 200.0}}],
            }
            (cache_dir / "valid1.json").write_text(json.dumps(valid_payload), encoding="utf-8")
            # 2. Duplicate file (same query coordinates + profile)
            (cache_dir / "valid2.json").write_text(json.dumps(valid_payload), encoding="utf-8")
            # 3. Corrupted file
            (cache_dir / "corrupted.json").write_text("{not json", encoding="utf-8")

            summary = process_route_cache(cache_dir, output_file)
            self.assertEqual(summary["discovered"], 3)
            self.assertEqual(summary["normalized"], 1)  # deduplicated to 1 record
            self.assertEqual(len(summary["skipped"]), 1)

            lines = output_file.read_text(encoding="utf-8").strip().split("\n")
            self.assertEqual(len(lines), 1)

    def test_process_weather_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            cache_dir.mkdir()
            output_file = Path(tmpdir) / "output" / "weather.jsonl"

            payload = {
                "latitude": 49.42,
                "longitude": 8.69,
                "hourly": {
                    "time": ["2026-10-12T00:00", "2026-10-12T01:00"],
                    "temperature_2m": [7.9, 8.1],
                    "precipitation": [0.0, 0.2],
                    "wind_speed_10m": [2.8, 3.5],
                    "visibility": [25720.0, 24000.0],
                },
            }
            (cache_dir / "weather1.json").write_text(json.dumps(payload), encoding="utf-8")

            summary = process_weather_cache(cache_dir, output_file)
            self.assertEqual(summary["discovered"], 1)
            self.assertEqual(summary["normalized"], 2)
            self.assertEqual(len(summary["skipped"]), 0)

            lines = output_file.read_text(encoding="utf-8").strip().split("\n")
            self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
