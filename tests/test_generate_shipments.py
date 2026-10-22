"""
test_generate_shipments.py – Unit tests for the order-linked carrier shipment generator.

Run locally with:
    python -m unittest tests.test_generate_shipments -v
"""

import json
import tempfile
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import GeneratorType
from unittest.mock import patch

from data_generators import generate_master_data, generate_orders
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import generate_order_lines, usable_master_keys
from data_generators.generate_shipments import (
    DEFAULT_REFERENCE_DATETIME,
    FAILURE_MODES,
    TS_FORMAT,
    build_network,
    generate_shipment_records,
    main,
    run,
    shippable_orders,
    weather_risk,
)

MASTER_COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}
ORDERS = 1500


def _ts(value):
    return datetime.strptime(value, TS_FORMAT)


def _build(bad_rate=0.02, seed=42):
    feeds, master_manifest = build_master_data(MASTER_COUNTS, seed=42, bad_rate=0.02)
    order_manifest = []
    lines = list(generate_order_lines(usable_master_keys(feeds, master_manifest), ORDERS, seed=42,
                                      bad_rate=0.02, manifest=order_manifest))
    network = build_network(feeds["carriers"], feeds["warehouses"], feeds["routes"], master_manifest)
    damaged_orders = {m["order_id"] for m in order_manifest}
    orders = list(shippable_orders(iter(lines), damaged_orders))
    manifest = []
    shipments = list(generate_shipment_records(iter(orders), network, seed=seed, bad_rate=bad_rate, manifest=manifest))
    return {"feeds": feeds, "master_manifest": master_manifest, "lines": lines, "order_manifest": order_manifest,
            "network": network, "orders": orders, "shipments": shipments, "manifest": manifest}


class TestShipmentGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = _build()
        cls.clean = _build(bad_rate=0.0)["shipments"]

    def test_deterministic(self):
        again = _build()
        self.assertEqual(self.data["shipments"], again["shipments"])
        self.assertEqual(self.data["manifest"], again["manifest"])

    def test_different_seed_changes_assignment(self):
        other = _build(seed=7)["shipments"]
        self.assertNotEqual([s["carrier_id"] for s in self.clean[:50]], [s["carrier_id"] for s in other[:50]])

    def test_generator_is_lazy(self):
        self.assertIsInstance(generate_shipment_records(iter([]), self.data["network"]), GeneratorType)

    def test_weather_risk_thresholds_match_guide(self):
        self.assertEqual(weather_risk(0.0, 10, 20), "LOW")
        self.assertEqual(weather_risk(3.0, 10, 20), "MODERATE")
        self.assertEqual(weather_risk(0.0, 35, 20), "MODERATE")
        self.assertEqual(weather_risk(8.0, 10, 20), "HIGH")
        self.assertEqual(weather_risk(0.0, 55, 20), "HIGH")
        self.assertEqual(weather_risk(0.0, 10, 0.5), "HIGH")

    def test_one_shipment_per_shipped_or_delivered_clean_order(self):
        damaged = {m["order_id"] for m in self.data["order_manifest"]}
        statuses = {}
        for line in self.data["lines"]:
            if line["order_id"] and line["order_id"] not in damaged:
                statuses[line["order_id"]] = line["order_status"]
        expected = {o for o, st in statuses.items() if st in ("SHIPPED", "DELIVERED")}
        self.assertEqual({s["order_id"] for s in self.clean}, expected)
        self.assertEqual(len(self.clean), len(expected))

    def test_shipments_never_reference_damaged_or_unknown_entities(self):
        network = self.data["network"]
        for s in self.clean:
            self.assertIn(s["carrier_id"], network["carriers"])
            route_ids = {r["route_id"] for r in network["routes"][s["warehouse_id"]]}
            self.assertIn(s["route_id"], route_ids, "route must start at the order's warehouse")

    def test_quantities_match_the_order(self):
        by_order = {o["order_id"]: o for o in self.data["orders"]}
        for s in self.clean:
            o = by_order[s["order_id"]]
            self.assertEqual(s["ordered_quantity"], o["ordered_quantity"])
            self.assertEqual(s["shipped_quantity"], o["shipped_quantity"])
            self.assertEqual(s["warehouse_id"], o["warehouse_id"])

    def test_timeline_is_consistent(self):
        ref = DEFAULT_REFERENCE_DATETIME.replace(tzinfo=None)
        for s in self.clean:
            dispatch, planned = _ts(s["dispatch_time_utc"]), _ts(s["planned_delivery_time_utc"])
            self.assertLess(dispatch, planned)
            self.assertLess(dispatch, ref)
            if s["shipment_status"] == "DELIVERED":
                actual = _ts(s["actual_delivery_time_utc"])
                self.assertLess(dispatch, actual)
                self.assertLessEqual(actual, ref)
                self.assertEqual(s["updated_at"], s["actual_delivery_time_utc"])
            else:
                self.assertEqual(s["shipment_status"], "IN_TRANSIT")
                self.assertIsNone(s["actual_delivery_time_utc"])

    def test_otif_baseline_is_realistic(self):
        delivered = [s for s in self.clean if s["shipment_status"] == "DELIVERED"]
        on_time = [s for s in delivered if s["actual_delivery_time_utc"] <= s["planned_delivery_time_utc"]]
        otif = [s for s in on_time if s["shipped_quantity"] >= s["ordered_quantity"]]
        self.assertTrue(0.75 < len(on_time) / len(delivered) < 0.97)
        self.assertTrue(0.65 < len(otif) / len(delivered) < 0.95)

    def test_high_weather_risk_lowers_on_time_rate(self):
        delivered = [s for s in self.clean if s["shipment_status"] == "DELIVERED"]
        def rate(risk):
            group = [s for s in delivered if s["weather_risk"] == risk]
            return sum(s["actual_delivery_time_utc"] <= s["planned_delivery_time_utc"] for s in group) / len(group)
        self.assertGreater(rate("LOW"), rate("HIGH"))

    def test_bad_rate_and_every_mode_injected(self):
        rate = len(self.data["manifest"]) / len(self.data["shipments"])
        self.assertTrue(0.008 < rate < 0.035, rate)
        self.assertEqual(set(Counter(m["failure_mode"] for m in self.data["manifest"])), set(FAILURE_MODES))

    def test_duplicate_mode_yields_identical_copy(self):
        for m in [m for m in self.data["manifest"] if m["failure_mode"] == "duplicate"]:
            copies = [s for s in self.data["shipments"] if s["shipment_id"] == m["shipment_id"]]
            self.assertEqual(len(copies), 2)
            self.assertEqual(copies[0], copies[1])

    def test_run_writes_file_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            generate_master_data.run(data_dir=data_dir, counts=MASTER_COUNTS, seed=42, bad_rate=0.02)
            generate_orders.run(data_dir=data_dir, order_count=300, seed=42, bad_rate=0.02)
            written = run(data_dir=data_dir)
            lines = (data_dir / "shipments" / "shipments.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), written)
            self.assertGreater(written, 0)
            manifest = json.loads((data_dir / "_manifests" / "shipments.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["rows_written"], written)

    def test_run_without_inputs_fails_clearly(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                run(data_dir=Path(tmp))

    @patch("sys.argv", ["generate_shipments.py", "--reference-time", "2026-10-20T06:00:00Z"])
    @patch("data_generators.generate_shipments.run")
    def test_cli_passes_reference_time_to_run(self, mock_run):
        """Shipments must use the same 'today' as the orders, or they would be dispatched before them."""
        main()
        self.assertEqual(mock_run.call_args.kwargs["reference_time"], datetime(2026, 10, 20, 6, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
