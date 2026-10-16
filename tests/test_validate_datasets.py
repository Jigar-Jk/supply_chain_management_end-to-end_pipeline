"""
test_validate_datasets.py – Unit tests for validate_datasets.

Tests individual validators (routes, weather, shipments) and cross-dataset validation
using isolated temporary fixtures.

Run with:
    python -m unittest tests.test_validate_datasets -v
"""

import json
import tempfile
import unittest
from pathlib import Path

from data_generators.validate_datasets import (
    validate_all_datasets,
    validate_routes,
    validate_shipments,
    validate_weather,
)


class TestValidateDatasets(unittest.TestCase):
    """Unit test suite for dataset validation logic."""

    def setUp(self):
        # Sample valid route
        self.valid_route = {
            "route_id": "route_001",
            "origin_longitude": 8.681495,
            "origin_latitude": 49.41461,
            "destination_longitude": 8.687872,
            "destination_latitude": 49.420318,
            "distance_meters": 1602.8,
            "duration_seconds": 284.6,
            "routing_profile": "driving-hgv",
            "source": "openrouteservice",
        }

        # Sample valid weather
        self.valid_weather = {
            "weather_record_id": "w_001",
            "latitude": 49.42275,
            "longitude": 8.69481,
            "forecast_time_utc": "2026-10-12T00:00",
            "temperature_2m_c": 7.9,
            "precipitation_mm": 0.0,
            "wind_speed_10m_kmh": 2.8,
            "visibility_m": 25720.0,
            "source": "open-meteo",
        }

        # Sample valid shipment
        self.valid_shipment = {
            "shipment_id": "shp_001",
            "route_id": "route_001",
            "carrier_id": "CARRIER_DHL",
            "dispatch_time_utc": "2026-09-28T15:34:04Z",
            "planned_delivery_time_utc": "2026-09-29T01:33:46Z",
            "data_type": "synthetic",
        }

    def _write_file(self, path: Path, records: list[dict] | list[str]) -> Path:
        content = "\n".join(
            r if isinstance(r, str) else json.dumps(r) for r in records
        ) + "\n"
        path.write_text(content, encoding="utf-8")
        return path

    # 1. All valid datasets pass validation
    def test_all_valid_datasets_pass(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [self.valid_route])
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [self.valid_weather])
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [self.valid_shipment])

            results = validate_all_datasets(r_file, w_file, s_file)
            self.assertEqual(results["overall_status"], "PASS")
            self.assertEqual(results["routes"]["valid_records"], 1)
            self.assertEqual(results["weather"]["valid_records"], 1)
            self.assertEqual(results["shipments"]["valid_records"], 1)

    # 2. Missing route file is detected
    def test_missing_route_file_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = Path(tmpdir) / "nonexistent_routes.jsonl"
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [self.valid_weather])
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [self.valid_shipment])

            results = validate_all_datasets(r_file, w_file, s_file)
            self.assertEqual(results["overall_status"], "FAIL")
            self.assertEqual(results["routes"]["status"], "FAIL")
            self.assertTrue(any("not found" in e.lower() for e in results["routes"]["errors"]))

    # 3. Missing weather file is detected
    def test_missing_weather_file_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [self.valid_route])
            w_file = Path(tmpdir) / "nonexistent_weather.jsonl"
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [self.valid_shipment])

            results = validate_all_datasets(r_file, w_file, s_file)
            self.assertEqual(results["overall_status"], "FAIL")
            self.assertEqual(results["weather"]["status"], "FAIL")
            self.assertTrue(any("not found" in e.lower() for e in results["weather"]["errors"]))

    # 4. Missing shipment file is detected
    def test_missing_shipment_file_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [self.valid_route])
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [self.valid_weather])
            s_file = Path(tmpdir) / "nonexistent_shipments.jsonl"

            results = validate_all_datasets(r_file, w_file, s_file)
            self.assertEqual(results["overall_status"], "FAIL")
            self.assertEqual(results["shipments"]["status"], "FAIL")
            self.assertTrue(any("not found" in e.lower() for e in results["shipments"]["errors"]))

    # 5. Malformed JSON is detected
    def test_malformed_json_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", ["{not valid json"])
            res = validate_routes(r_file)
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("malformed json" in e.lower() for e in res["errors"]))

    # 6. Duplicate route IDs are detected
    def test_duplicate_route_ids_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(
                Path(tmpdir) / "routes.jsonl", [self.valid_route, self.valid_route]
            )
            res = validate_routes(r_file)
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("duplicate 'route_id'" in e.lower() for e in res["errors"]))
            self.assertIn("route_001", res["duplicate_route_ids"])

    # 7. Duplicate shipment IDs are detected
    def test_duplicate_shipment_ids_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            s_file = self._write_file(
                Path(tmpdir) / "shipments.jsonl", [self.valid_shipment, self.valid_shipment]
            )
            res = validate_shipments(s_file, valid_route_ids={"route_001"})
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("duplicate 'shipment_id'" in e.lower() for e in res["errors"]))
            self.assertIn("shp_001", res["duplicate_shipment_ids"])

    # 8. Shipment referencing a nonexistent route is detected
    def test_shipment_referencing_nonexistent_route_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_shipment = dict(self.valid_shipment)
            bad_shipment["route_id"] = "route_does_not_exist"
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [bad_shipment])
            res = validate_shipments(s_file, valid_route_ids={"route_001"})
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["nonexistent_route_shipments"], 1)
            self.assertTrue(any("referential integrity" in e.lower() for e in res["errors"]))

    # 9. Planned delivery before or equal to dispatch is detected
    def test_delivery_before_or_equal_to_dispatch_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Case 1: earlier
            bad1 = dict(self.valid_shipment)
            bad1["planned_delivery_time_utc"] = "2026-09-20T00:00:00Z"  # before dispatch
            # Case 2: equal
            bad2 = dict(self.valid_shipment)
            bad2["planned_delivery_time_utc"] = bad2["dispatch_time_utc"]

            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [bad1, bad2])
            res = validate_shipments(s_file, valid_route_ids={"route_001"})
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("strictly later" in e.lower() for e in res["errors"]))

    # 10. Invalid coordinates are detected
    def test_invalid_coordinates_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Bad lat (> 90)
            bad_route = dict(self.valid_route)
            bad_route["origin_latitude"] = 99.0
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [bad_route])
            res = validate_routes(r_file)
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("outside valid range" in e.lower() for e in res["errors"]))

            # Bad lon in weather (> 180)
            bad_weather = dict(self.valid_weather)
            bad_weather["longitude"] = 250.0
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [bad_weather])
            w_res = validate_weather(w_file)
            self.assertEqual(w_res["status"], "FAIL")
            self.assertTrue(any("outside valid range" in e.lower() for e in w_res["errors"]))

    # 11. Invalid weather values are detected
    def test_invalid_weather_values_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Negative precipitation or non-numeric
            bad_w1 = dict(self.valid_weather)
            bad_w1["precipitation_mm"] = -2.5

            bad_w2 = dict(self.valid_weather)
            bad_w2["weather_record_id"] = "w_002"
            bad_w2["wind_speed_10m_kmh"] = "fast"

            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [bad_w1, bad_w2])
            res = validate_weather(w_file)
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("cannot be negative" in e.lower() for e in res["errors"]))
            self.assertTrue(any("must be numeric" in e.lower() for e in res["errors"]))

    # 12. Invalid timestamps are detected
    def test_invalid_timestamps_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_shipment = dict(self.valid_shipment)
            bad_shipment["dispatch_time_utc"] = "not-a-timestamp"
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [bad_shipment])
            res = validate_shipments(s_file, valid_route_ids={"route_001"})
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["invalid_timestamp_records"], 1)

    # 13. Missing required fields are detected
    def test_missing_required_fields_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_route = dict(self.valid_route)
            del bad_route["distance_meters"]
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [bad_route])
            res = validate_routes(r_file)
            self.assertEqual(res["status"], "FAIL")
            self.assertTrue(any("missing required field 'distance_meters'" in e.lower() for e in res["errors"]))

    # 14. Valid datasets with only one route pass
    def test_single_route_multiple_shipments_pass(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [self.valid_route])
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [self.valid_weather])

            # Two shipments referencing the single route
            s1 = dict(self.valid_shipment)
            s2 = dict(self.valid_shipment)
            s2["shipment_id"] = "shp_002"

            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [s1, s2])

            results = validate_all_datasets(r_file, w_file, s_file)
            self.assertEqual(results["overall_status"], "PASS")
            self.assertEqual(results["routes"]["valid_records"], 1)
            self.assertEqual(results["shipments"]["valid_records"], 2)
            self.assertEqual(len(results["shipments"]["referenced_route_ids"]), 1)

    # 15. The validator does not modify the source files
    def test_validator_does_not_modify_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            r_file = self._write_file(Path(tmpdir) / "routes.jsonl", [self.valid_route])
            w_file = self._write_file(Path(tmpdir) / "weather.jsonl", [self.valid_weather])
            s_file = self._write_file(Path(tmpdir) / "shipments.jsonl", [self.valid_shipment])

            r_before = r_file.read_bytes()
            w_before = w_file.read_bytes()
            s_before = s_file.read_bytes()

            validate_all_datasets(r_file, w_file, s_file)

            self.assertEqual(r_file.read_bytes(), r_before)
            self.assertEqual(w_file.read_bytes(), w_before)
            self.assertEqual(s_file.read_bytes(), s_before)


if __name__ == "__main__":
    unittest.main()
