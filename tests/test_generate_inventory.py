"""
test_generate_inventory.py – Unit tests for the WMS nightly stock reconciliation generator.

Run locally with:
    python -m unittest tests.test_generate_inventory -v
"""

import json
import tempfile
import unittest
from collections import Counter
from datetime import date
from pathlib import Path
from types import GeneratorType

from data_generators import generate_master_data
from data_generators.generate_inventory import (
    FAILURE_MODES,
    UNKNOWN_PRODUCT_PREFIX,
    UNKNOWN_WAREHOUSE_PREFIX,
    generate_inventory_rows,
    run,
    snapshot_nights,
)
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import usable_master_keys

MASTER_COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}
NIGHTS = snapshot_nights(date(2026, 10, 12), 3)
SKUS = 60


def _generate(bad_rate=0.02):
    feeds, master_manifest = build_master_data(MASTER_COUNTS, seed=42, bad_rate=0.02)
    keys = usable_master_keys(feeds, master_manifest)
    manifest = []
    rows = list(generate_inventory_rows(keys, NIGHTS, SKUS, seed=42, bad_rate=bad_rate, manifest=manifest))
    return keys, rows, manifest, master_manifest


class TestInventoryGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.keys, cls.rows, cls.manifest, cls.master_manifest = _generate()
        _, cls.clean_rows, _, _ = _generate(bad_rate=0.0)
        cls.clean = [r for _, r in cls.clean_rows]

    def test_deterministic(self):
        _, rows2, manifest2, _ = _generate()
        self.assertEqual(self.rows, rows2)
        self.assertEqual(self.manifest, manifest2)

    def test_generator_is_lazy(self):
        self.assertIsInstance(generate_inventory_rows(self.keys, NIGHTS, 5), GeneratorType)

    def test_snapshot_nights_are_consecutive_oldest_first(self):
        self.assertEqual(NIGHTS, [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12)])

    def test_grain_is_warehouse_product_night(self):
        keys = [(r["warehouse_id"], r["product_id"], r["snapshot_date"]) for r in self.clean]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(keys), len(self.keys["warehouses"]) * SKUS * len(NIGHTS))

    def test_damaged_master_records_are_never_referenced(self):
        damaged = {(m["feed"], m["original_key"]) for m in self.master_manifest if m["failure_mode"] != "duplicate"}
        used_wh = {r["warehouse_id"] for r in self.clean}
        used_sku = {r["product_id"] for r in self.clean}
        self.assertFalse({("warehouses", k) for k in used_wh} & damaged)
        self.assertFalse({("products", k) for k in used_sku} & damaged)

    def test_clean_rows_respect_invariants(self):
        for r in self.clean:
            self.assertTrue(0 <= r["quantity_reserved"] <= r["quantity_on_hand"])
            self.assertLess(r["safety_stock"], r["reorder_point"])
            # Exported before the 02:00 WMS SLA on the snapshot date itself
            self.assertTrue(r["recorded_at"].startswith(r["snapshot_date"] + "T01:"))

    def test_stockouts_and_risks_occur(self):
        available = [(r["quantity_on_hand"] - r["quantity_reserved"], r["safety_stock"]) for r in self.clean]
        stockout = sum(1 for a, _ in available if a <= 0) / len(available)
        at_risk = sum(1 for a, s in available if a <= s) / len(available)
        self.assertTrue(0.005 < stockout < 0.15, stockout)
        self.assertGreater(at_risk, stockout)

    def test_bad_rate_and_every_mode_injected(self):
        rate = len(self.manifest) / len(self.rows)
        self.assertTrue(0.012 < rate < 0.03, rate)
        self.assertEqual(set(Counter(m["failure_mode"] for m in self.manifest)), set(FAILURE_MODES))
        values = [r for _, r in self.rows]
        self.assertTrue(any((r["warehouse_id"] or "").startswith(UNKNOWN_WAREHOUSE_PREFIX) for r in values))
        self.assertTrue(any((r["product_id"] or "").startswith(UNKNOWN_PRODUCT_PREFIX) for r in values))

    def test_corrupted_rows_never_share_a_grain_key(self):
        """Regression: with one shared unknown code, several bad rows collided on one key and all but
        the newest were treated as superseded versions, so the quarantine undercounted bad data."""
        keys = [(r["warehouse_id"], r["product_id"], r["snapshot_date"]) for _, r in self.rows
                if r["warehouse_id"] and r["product_id"]]
        duplicate_copies = sum(1 for m in self.manifest if m["failure_mode"] == "duplicate")
        self.assertEqual(len(keys) - len(set(keys)), duplicate_copies,
                         "only the deliberate duplicates may share a key")

    def test_corrupted_future_row_stays_in_its_original_nights_file(self):
        future = [(night, r) for night, r in self.rows if r["snapshot_date"] > "2030-01-01"]
        self.assertTrue(future)
        for night, _ in future:
            self.assertIn(night, {n.isoformat() for n in NIGHTS})

    def test_duplicate_mode_yields_identical_copy(self):
        for m in [m for m in self.manifest if m["failure_mode"] == "duplicate"][:10]:
            copies = [r for _, r in self.rows if (r["warehouse_id"], r["product_id"], r["snapshot_date"])
                      == (m["warehouse_id"], m["product_id"], m["snapshot_date"])]
            self.assertEqual(len(copies), 2)
            self.assertEqual(copies[0], copies[1])

    def test_run_writes_one_file_per_night_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            generate_master_data.run(data_dir=data_dir, counts=MASTER_COUNTS, seed=42, bad_rate=0.02)
            written = run(data_dir=data_dir, nights=2, skus_per_warehouse=10, last_night=date(2026, 10, 12))
            self.assertEqual(set(written), {"2026-10-11", "2026-10-12"})
            for night, n in written.items():
                lines = (data_dir / "inventory" / f"inventory_{night}.jsonl").read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(lines), n)
            manifest = json.loads((data_dir / "_manifests" / "inventory.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["rows_written"], written)


if __name__ == "__main__":
    unittest.main()
