"""
test_generate_orders.py – Unit tests for the synthetic SQL Server OMS order-line generator.

Run locally with:
    python -m unittest tests.test_generate_orders -v
"""

import json
import tempfile
import unittest
from collections import Counter
from datetime import date
from pathlib import Path
from types import GeneratorType

from data_generators import generate_master_data
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import (
    DEFAULT_ASSORTMENT_SIZE,
    DEFAULT_REFERENCE_DATETIME,
    FAILURE_MODES,
    UNKNOWN_CUSTOMER_ID,
    UNKNOWN_PRODUCT_ID,
    UNKNOWN_WAREHOUSE_ID,
    generate_order_lines,
    run,
    usable_master_keys,
    warehouse_assortment,
)

MASTER_COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}
ORDERS = 1500


def _generate(seed=42, bad_rate=0.02):
    feeds, master_manifest = build_master_data(MASTER_COUNTS, seed=42, bad_rate=0.02)
    keys = usable_master_keys(feeds, master_manifest)
    manifest = []
    lines = list(generate_order_lines(keys, ORDERS, seed=seed, bad_rate=bad_rate, manifest=manifest))
    return keys, lines, manifest, master_manifest


class TestOrderGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.keys, cls.lines, cls.manifest, cls.master_manifest = _generate()
        cls.clean_keys, cls.clean_lines, _, _ = _generate(bad_rate=0.0)

    def test_deterministic_for_same_seed(self):
        _, lines2, manifest2, _ = _generate()
        self.assertEqual(self.lines, lines2)
        self.assertEqual(self.manifest, manifest2)

    def test_generator_is_lazy(self):
        self.assertIsInstance(generate_order_lines(self.keys, 10), GeneratorType)

    def test_damaged_master_records_are_never_referenced(self):
        damaged = {(m["feed"], m["original_key"]) for m in self.master_manifest if m["failure_mode"] != "duplicate"}
        for feed, field in [("customers", "customer_id"), ("products", "product_id"), ("warehouses", "warehouse_id")]:
            used = {line[field] for line in self.clean_lines}
            self.assertFalse({(feed, k) for k in used} & damaged, feed)
            self.assertTrue(used <= set(self.keys[feed]), feed)

    def test_products_come_from_the_warehouse_assortment(self):
        for line in self.clean_lines:
            assortment = warehouse_assortment(self.clean_keys, line["warehouse_id"], DEFAULT_ASSORTMENT_SIZE, 42)
            self.assertIn(line["product_id"], assortment)

    def test_assortment_is_deterministic_and_bounded(self):
        a = warehouse_assortment(self.keys, "WH-0001", 25, 42)
        self.assertEqual(a, warehouse_assortment(self.keys, "WH-0001", 25, 42))
        self.assertEqual(len(a), len(set(a)), 25)
        self.assertNotEqual(a, warehouse_assortment(self.keys, "WH-0002", 25, 42))
        self.assertEqual(len(warehouse_assortment(self.keys, "WH-0001", 10**6, 42)), len(self.keys["products"]))

    def test_lines_per_order_and_line_numbers(self):
        by_order = Counter(line["order_id"] for line in self.clean_lines)
        self.assertEqual(len(by_order), ORDERS)
        self.assertTrue(all(1 <= n <= 5 for n in by_order.values()))
        for order_id in list(by_order)[:50]:
            numbers = sorted(l["order_line_number"] for l in self.clean_lines if l["order_id"] == order_id)
            self.assertEqual(numbers, list(range(1, len(numbers) + 1)))

    def test_clean_lines_respect_business_invariants(self):
        ref = DEFAULT_REFERENCE_DATETIME.date()
        for line in self.clean_lines:
            ordered = date.fromisoformat(line["order_date"])
            self.assertLessEqual(ordered, ref)
            self.assertGreaterEqual(date.fromisoformat(line["promised_delivery_date"]), ordered)
            self.assertGreater(line["ordered_quantity"], 0)
            self.assertTrue(0 <= line["shipped_quantity"] <= line["ordered_quantity"])
            if line["order_status"] not in ("SHIPPED", "DELIVERED"):
                self.assertEqual(line["shipped_quantity"], 0)
            self.assertGreater(line["unit_price"], 0)

    def test_status_mix_and_short_shipments(self):
        statuses = Counter(line["order_status"] for line in self.clean_lines)
        self.assertEqual(set(statuses), {"CREATED", "PICKED", "SHIPPED", "DELIVERED", "CANCELLED"})
        shipped = [l for l in self.clean_lines if l["order_status"] in ("SHIPPED", "DELIVERED")]
        fill = sum(l["shipped_quantity"] for l in shipped) / sum(l["ordered_quantity"] for l in shipped)
        self.assertTrue(0.9 < fill < 1.0, fill)

    def test_bad_rate_and_every_mode_injected(self):
        rate = len(self.manifest) / len(self.lines)
        self.assertTrue(0.012 < rate < 0.03, rate)
        modes = Counter(m["failure_mode"] for m in self.manifest)
        self.assertEqual(set(modes), set(FAILURE_MODES))

    def test_injected_values(self):
        self.assertTrue(any(l["customer_id"] == UNKNOWN_CUSTOMER_ID for l in self.lines))
        self.assertTrue(any(l["product_id"] == UNKNOWN_PRODUCT_ID for l in self.lines))
        self.assertTrue(any(l["warehouse_id"] == UNKNOWN_WAREHOUSE_ID for l in self.lines))
        self.assertTrue(any(l["order_id"] is None for l in self.lines))

    def test_duplicate_mode_yields_identical_copy(self):
        dupes = [m for m in self.manifest if m["failure_mode"] == "duplicate"]
        self.assertTrue(dupes)
        for m in dupes:
            copies = [l for l in self.lines
                      if l["order_id"] == m["order_id"] and l["order_line_number"] == m["order_line_number"]]
            self.assertEqual(len(copies), 2)
            self.assertEqual(copies[0], copies[1])

    def test_zero_bad_rate_has_unique_line_keys(self):
        keys = [(l["order_id"], l["order_line_number"]) for l in self.clean_lines]
        self.assertEqual(len(keys), len(set(keys)))

    def test_run_writes_orders_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            generate_master_data.run(data_dir=data_dir, counts=MASTER_COUNTS, seed=42, bad_rate=0.02)
            written = run(data_dir=data_dir, order_count=200, seed=42, bad_rate=0.02)
            lines = (data_dir / "orders" / "orders.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), written)
            manifest = json.loads((data_dir / "_manifests" / "orders.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["lines_written"], written)

    def test_run_without_master_data_fails_clearly(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                run(data_dir=Path(tmp), order_count=10)


if __name__ == "__main__":
    unittest.main()
