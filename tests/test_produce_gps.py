"""
test_produce_gps.py – Unit tests for the GPS/RFID telemetry producer (no broker needed).

Run locally with:
    python -m unittest tests.test_produce_gps -v
"""

import json
import os
import tempfile
import unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import generate_order_lines, usable_master_keys
from data_generators.generate_shipments import build_network, generate_shipment_records, shippable_orders
from data_generators.produce_gps import (
    DARK_LAG,
    FAILURE_RATES,
    FileSink,
    build_fleet,
    generate_messages,
    kafka_config,
    run,
)

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def fixed_clock():
    return NOW


def build_inputs(orders=1500):
    counts = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}
    feeds, master_manifest = build_master_data(counts, seed=42, bad_rate=0.02)
    keys = usable_master_keys(feeds, master_manifest)
    order_manifest, shipment_manifest = [], []
    lines = list(generate_order_lines(keys, orders, seed=42, bad_rate=0.02, manifest=order_manifest))
    network = build_network(feeds["carriers"], feeds["warehouses"], feeds["routes"], master_manifest)
    shipments = list(generate_shipment_records(
        shippable_orders(iter(lines), {m["order_id"] for m in order_manifest}), network,
        seed=42, bad_rate=0.02, manifest=shipment_manifest))
    damaged = {m["shipment_id"] for m in shipment_manifest if m["failure_mode"] != "duplicate"}
    return feeds, shipments, damaged


class TestGpsProducer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.feeds, cls.shipments, cls.damaged = build_inputs()
        cls.fleet = build_fleet(cls.shipments, cls.feeds["routes"], cls.damaged, seed=42)
        cls.manifest = []
        cls.messages = list(generate_messages(
            build_fleet(cls.shipments, cls.feeds["routes"], cls.damaged, seed=42),
            20000, seed=42, clock=fixed_clock, manifest=cls.manifest))

    def test_fleet_is_clean_in_transit_shipments_only(self):
        in_transit = {s["shipment_id"] for s in self.shipments
                      if s["shipment_status"] == "IN_TRANSIT" and s["shipment_id"] and s["shipment_id"] not in self.damaged}
        self.assertEqual({t["shipment_id"] for t in self.fleet}, in_transit)
        self.assertEqual(len({t["truck_id"] for t in self.fleet}), len(self.fleet))

    def test_some_trucks_are_dark(self):
        dark = sum(t["dark"] for t in self.fleet)
        self.assertTrue(0 < dark < len(self.fleet))

    def test_deterministic_with_fixed_clock(self):
        again = list(generate_messages(build_fleet(self.shipments, self.feeds["routes"], self.damaged, seed=42),
                                       20000, seed=42, clock=fixed_clock))
        self.assertEqual(self.messages, again)

    def test_clean_events_follow_the_contract(self):
        bad_ids = {m["event_id"] for m in self.manifest if m["failure_mode"] != "duplicate"}
        trucks = {t["truck_id"]: t for t in self.fleet}
        for message in self.messages[:3000]:
            if message.startswith("{not-json"):
                continue
            e = json.loads(message)
            if e["event_id"] in bad_ids:
                continue
            self.assertIn(e["event_type"], ("GPS", "RFID"))
            self.assertEqual(e["pallet_id"] is not None, e["event_type"] == "RFID")
            truck = trucks[e["truck_id"]]
            self.assertEqual(e["shipment_id"], truck["shipment_id"])
            (lat0, lon0), (lat1, lon1) = truck["origin"], truck["destination"]
            self.assertTrue(min(lat0, lat1) - 0.01 <= e["lat"] <= max(lat0, lat1) + 0.01)
            self.assertTrue(min(lon0, lon1) - 0.01 <= e["lon"] <= max(lon0, lon1) + 0.01)
            expected_ts = NOW - DARK_LAG if truck["dark"] else NOW
            self.assertTrue(e["ts"].startswith(expected_ts.strftime("%Y-%m-%dT%H:%M:%S")))

    def test_failure_modes_and_rates(self):
        modes = Counter(m["failure_mode"] for m in self.manifest)
        self.assertEqual(set(modes), set(FAILURE_RATES))
        rate = len(self.manifest) / 20000
        self.assertTrue(0.018 < rate < 0.032, rate)
        unparseable = [m for m in self.messages if m.startswith("{not-json")]
        self.assertEqual(len(unparseable), modes["unparseable"])
        parsed = [json.loads(m) for m in self.messages if not m.startswith("{not-json")]
        self.assertEqual(sum(1 for e in parsed if e["lat"] == 999.0), modes["lat_out_of_range"])
        self.assertEqual(sum(1 for e in parsed if "truck_id" not in e), modes["missing_truck_id"])

    def test_duplicates_are_identical_redeliveries(self):
        dup_ids = {m["event_id"] for m in self.manifest if m["failure_mode"] == "duplicate"}
        counts = Counter(json.loads(m)["event_id"] for m in self.messages if not m.startswith("{not-json"))
        for event_id in dup_ids:
            self.assertEqual(counts[event_id], 2)
        self.assertEqual(len(self.messages), 20000 + len(dup_ids))

    def test_kafka_config_local_vs_event_hubs(self):
        with patch.dict(os.environ, {}, clear=True):
            local = kafka_config("localhost:9092")
        self.assertEqual(local["bootstrap.servers"], "localhost:9092")
        self.assertNotIn("security.protocol", local)
        with patch.dict(os.environ, {"EVENTHUB_CONNECTION_STRING": "Endpoint=sb://x/;SharedAccessKey=k"}):
            eh = kafka_config("ehns-scm-dev.servicebus.windows.net:9093")
        self.assertEqual(eh["security.protocol"], "SASL_SSL")
        self.assertEqual(eh["sasl.username"], "$ConnectionString")
        self.assertEqual(eh["sasl.password"], "Endpoint=sb://x/;SharedAccessKey=k")

    def test_file_sink_run_writes_messages_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "gps.jsonl"
            fleet = build_fleet(self.shipments, self.feeds["routes"], self.damaged, seed=42)
            sent = run(FileSink(out), fleet, 500, eps=0, seed=42, data_dir=Path(tmp))
            self.assertEqual(len(out.read_text(encoding="utf-8").splitlines()), sent)
            manifest = json.loads((Path(tmp) / "_manifests" / "gps_events.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["messages_sent"], sent)


if __name__ == "__main__":
    unittest.main()
