"""
test_generate_master_data.py – Unit tests for the synthetic master data generator.

Run locally with:
    python -m unittest tests.test_generate_master_data -v
"""

import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from data_generators.generate_master_data import (
    CORRUPTIONS,
    OUTPUT_FILES,
    SCALE_PRESETS,
    build_master_data,
    haversine_km,
    route_id_for,
    run,
)
from data_generators.normalize_api_data import normalize_route

SMALL_COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}

try:
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYSPARK_AVAILABLE = False


class TestMasterDataGenerator(unittest.TestCase):

    def setUp(self):
        self.feeds, self.manifest = build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.02)

    def test_deterministic_for_same_seed(self):
        feeds2, manifest2 = build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.02)
        self.assertEqual(self.feeds, feeds2)
        self.assertEqual(self.manifest, manifest2)

    def test_different_seed_changes_output(self):
        feeds2, _ = build_master_data(SMALL_COUNTS, seed=7, bad_rate=0.02)
        self.assertNotEqual(self.feeds["products"], feeds2["products"])

    def test_changing_one_volume_does_not_reshuffle_other_feeds(self):
        bigger = dict(SMALL_COUNTS, products=600)
        feeds2, _ = build_master_data(bigger, seed=42, bad_rate=0.02)
        self.assertEqual(self.feeds["customers"], feeds2["customers"])
        self.assertEqual(self.feeds["warehouses"], feeds2["warehouses"])

    def test_row_counts_equal_requested_plus_duplicates(self):
        for feed, requested in SMALL_COUNTS.items():
            dupes = sum(1 for m in self.manifest if m["feed"] == feed and m["failure_mode"] == "duplicate")
            self.assertEqual(len(self.feeds[feed]), requested + dupes, feed)

    def test_bad_rate_is_about_two_percent(self):
        for feed, requested in SMALL_COUNTS.items():
            n_bad = sum(1 for m in self.manifest if m["feed"] == feed)
            self.assertEqual(n_bad, max(1, round(requested * 0.02)), feed)

    def test_every_failure_mode_used_when_feed_is_large_enough(self):
        modes = Counter((m["feed"], m["failure_mode"]) for m in self.manifest)
        for feed in ("products", "customers", "routes"):
            for mode, _ in CORRUPTIONS[feed][1]:
                self.assertGreater(modes[(feed, mode)], 0, f"{feed}/{mode} never injected")

    def test_zero_bad_rate_produces_clean_unique_keys(self):
        feeds, manifest = build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.0)
        self.assertEqual(manifest, [])
        for feed, (key, _) in CORRUPTIONS.items():
            keys = [r[key] for r in feeds[feed]]
            self.assertTrue(all(keys), f"{feed} has empty keys with bad_rate=0")
            self.assertEqual(len(keys), len(set(keys)), f"{feed} has duplicate keys with bad_rate=0")

    def test_route_id_matches_normalize_api_data_contract(self):
        ors_payload = {
            "metadata": {"query": {"coordinates": [[8.681495, 49.41461], [8.687872, 49.420318]], "profile": "driving-hgv"}},
            "routes": [{"summary": {"distance": 1602.8, "duration": 284.6}}],
        }
        expected = normalize_route(ors_payload, "test.json")["route_id"]
        self.assertEqual(route_id_for(8.681495, 49.41461, 8.687872, 49.420318), expected)
        # Same ID as the real cached ORS route in data/routes/routes.jsonl
        self.assertEqual(expected, "ce9844de20b7dc9b")

    def test_routes_originate_at_generated_warehouses_with_plausible_metrics(self):
        clean, _ = build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.0)
        origins = {(w["longitude"], w["latitude"]) for w in clean["warehouses"]}
        for r in clean["routes"]:
            self.assertIn((r["origin_longitude"], r["origin_latitude"]), origins)
            self.assertEqual(r["route_id"], route_id_for(
                r["origin_longitude"], r["origin_latitude"], r["destination_longitude"], r["destination_latitude"]))
            self.assertGreater(r["distance_meters"], 0)
            km = r["distance_meters"] / 1000
            straight = haversine_km(r["origin_latitude"], r["origin_longitude"],
                                    r["destination_latitude"], r["destination_longitude"])
            self.assertGreaterEqual(km + 1e-6, straight)
            self.assertLess(km, 2500)

    def test_products_price_above_cost(self):
        clean, _ = build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.0)
        for p in clean["products"]:
            self.assertGreater(p["unit_price"], p["unit_cost"])

    def test_legacy_carrier_ids_preserved_for_existing_shipments(self):
        ids = [c["carrier_id"] for c in build_master_data(SMALL_COUNTS, seed=42, bad_rate=0.0)[0]["carriers"]]
        for legacy in ["CARRIER_DHL", "CARRIER_FEDEX", "CARRIER_UPS", "CARRIER_DB_SCHENKER", "CARRIER_KUHNE_NAGEL"]:
            self.assertIn(legacy, ids)

    @unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required to compare against Bronze schemas")
    def test_record_fields_match_bronze_schemas(self):
        for feed in ("warehouses", "products", "customers", "carriers", "routes"):
            schema_fields = {f.name for f in DATASET_SCHEMAS[feed]().fields}
            for rec in self.feeds[feed][:20]:
                self.assertEqual(set(rec), schema_fields, feed)

    def test_run_writes_landing_files_and_manifest_outside_landing(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            written = run(data_dir=data_dir, counts=SMALL_COUNTS, seed=42, bad_rate=0.02)
            for feed, rel in OUTPUT_FILES.items():
                lines = (data_dir / rel).read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(lines), written[feed])
                json.loads(lines[0])
            manifest = json.loads((data_dir / "_manifests" / "master_data.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["rows_written"], written)
            self.assertEqual(len(manifest["corrupted_records"]), len(self.manifest))
            # Routes go to a separate file so the real ORS seed in routes.jsonl is untouched
            self.assertFalse((data_dir / "routes" / "routes.jsonl").exists())

    def test_scale_presets_include_guide_targets(self):
        self.assertEqual(SCALE_PRESETS["full"]["products"], 500_000)
        self.assertEqual(SCALE_PRESETS["full"]["warehouses"], 200)


if __name__ == "__main__":
    unittest.main()
