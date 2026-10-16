"""
test_master_data_to_silver.py – Generated master data and orders through the real Silver transforms.

Checks that every record the generators corrupted is quarantined (none silently dropped,
none leaking into a dimension or fact), under the expected rule, and that clean records all land.

Run locally with:
    python -m unittest tests.test_master_data_to_silver -v
"""

import json
import unittest
from collections import Counter
from datetime import date, datetime

from data_generators.generate_master_data import CORRUPTIONS, build_master_data
from data_generators.generate_inventory import generate_inventory_rows, snapshot_nights
from data_generators.generate_orders import generate_order_lines, usable_master_keys
from data_generators.generate_shipments import build_network, generate_shipment_records, shippable_orders

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform import transform_silver as ts
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    from scm_platform.transform_silver import (
        transform_fact_inventory_snapshot,
        transform_fact_order,
        transform_fact_shipment,
    )

COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}

# Generator failure mode -> the Silver rule that must catch it.
EXPECTED_RULE = {
    ("warehouses", "null_key"): "RULE_WAREHOUSE_ID_MISSING",
    ("warehouses", "latitude_out_of_range"): "RULE_WAREHOUSE_COORDINATES",
    ("warehouses", "duplicate"): "RULE_DUPLICATE_KEY",
    ("products", "null_key"): "RULE_PRODUCT_ID_MISSING",
    ("products", "non_positive_weight"): "RULE_PRODUCT_WEIGHT_NON_POSITIVE",
    ("products", "zero_volume"): "RULE_PRODUCT_VOLUME_NON_POSITIVE",
    ("products", "duplicate"): "RULE_DUPLICATE_KEY",
    ("customers", "empty_key"): "RULE_CUSTOMER_ID_EMPTY",
    ("customers", "null_key"): "RULE_CUSTOMER_ID_EMPTY",
    ("customers", "duplicate"): "RULE_DUPLICATE_KEY",
    ("carriers", "null_key"): "RULE_CARRIER_ID_EMPTY",
    ("carriers", "empty_key"): "RULE_CARRIER_ID_EMPTY",
    ("routes", "negative_distance"): "RULE_ROUTE_DISTANCE_NON_POSITIVE",
    ("routes", "latitude_out_of_range"): "RULE_ROUTE_COORDINATES",
    ("routes", "zero_duration"): "RULE_ROUTE_DURATION_NON_POSITIVE",
    ("routes", "null_key"): "RULE_ROUTE_ID_MISSING",
    ("routes", "duplicate"): "RULE_DUPLICATE_KEY",
}

ORDERS = 1500
INVENTORY_NIGHTS = snapshot_nights(date(2026, 10, 12), 3)
INVENTORY_SKUS = 40

EXPECTED_INVENTORY_RULE = {
    "null_warehouse_id": "RULE_INVENTORY_WAREHOUSE_ID_MISSING",
    "null_product_id": "RULE_INVENTORY_PRODUCT_ID_MISSING",
    "negative_on_hand": "RULE_QTY_ON_HAND_NON_NEGATIVE",
    "reserved_exceeds_on_hand": "RULE_QTY_RESERVED_INVALID",
    "future_snapshot_date": "RULE_SNAPSHOT_DATE_IN_FUTURE",
    "unknown_warehouse": "RULE_REF_INTEGRITY_WAREHOUSE",
    "unknown_product": "RULE_REF_INTEGRITY_PRODUCT",
    "duplicate": "RULE_DUPLICATE_KEY",
}

EXPECTED_ORDER_RULE = {
    "null_order_id": "RULE_ORDER_ID_MISSING",
    "non_positive_quantity": "RULE_ORDER_QUANTITY_POSITIVE",
    "negative_price": "RULE_UNIT_PRICE_NON_NEGATIVE",
    "promised_before_order": "RULE_PROMISED_BEFORE_ORDER",
    "shipped_exceeds_ordered": "RULE_SHIPPED_EXCEEDS_ORDERED",
    "future_order_date": "RULE_ORDER_DATE_IN_FUTURE",
    "unknown_customer": "RULE_REF_INTEGRITY_CUSTOMER",
    "unknown_product": "RULE_REF_INTEGRITY_PRODUCT",
    "unknown_warehouse": "RULE_REF_INTEGRITY_WAREHOUSE",
    "duplicate": "RULE_DUPLICATE_KEY",
}

EXPECTED_SHIPMENT_RULE = {
    "null_shipment_id": "RULE_SHIPMENT_ID_MISSING",
    "unparseable_dispatch": "RULE_TIMESTAMP_PARSE",
    "planned_before_dispatch": "RULE_CHRONOLOGICAL_DISPATCH",
    "actual_before_dispatch": "RULE_ACTUAL_BEFORE_DISPATCH",
    "delivered_without_timestamp": "RULE_DELIVERED_WITHOUT_TIMESTAMP",
    "shipped_exceeds_ordered": "RULE_SHIPPED_EXCEEDS_ORDERED",
    "unknown_carrier": "RULE_REF_INTEGRITY_CARRIER",
    "unknown_route": "RULE_REF_INTEGRITY_ROUTE",
    "unknown_warehouse": "RULE_REF_INTEGRITY_WAREHOUSE",
    "duplicate": "RULE_DUPLICATE_KEY",
}

DIM_TRANSFORMS = {
    "warehouses": "transform_dim_warehouse",
    "products": "transform_dim_product",
    "customers": "transform_dim_customer",
    "carriers": "transform_dim_carrier",
    "routes": "transform_dim_route",
}


class TestMasterDataToSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            return
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("UnitTestMasterDataToSilver")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.feeds, cls.manifest = build_master_data(COUNTS, seed=42, bad_rate=0.02)
        cls.results = {}
        dim_dfs = {}
        for feed, fn_name in DIM_TRANSFORMS.items():
            bronze = cls.spark.createDataFrame(cls.feeds[feed], schema=DATASET_SCHEMAS[feed]()) \
                .withColumn("_ingest_timestamp", lit(datetime(2026, 10, 13, 2, 0)))
            dim_df, quar_df = getattr(ts, fn_name)(bronze, batch_id="b_test")
            dim_dfs[feed] = dim_df.cache()
            cls.results[feed] = (dim_df.collect(), quar_df.collect())
        cls.dim_dfs = dim_dfs

        # Orders reference the same generated master data; dims are loaded after the orders
        # were placed, exercising the point-in-time "first version" lookup.
        keys = usable_master_keys(cls.feeds, cls.manifest)
        cls.order_manifest = []
        cls.order_lines = list(generate_order_lines(keys, ORDERS, seed=42, bad_rate=0.02,
                                                    manifest=cls.order_manifest))
        bronze_orders = cls.spark.createDataFrame(cls.order_lines, schema=DATASET_SCHEMAS["orders"]()) \
            .withColumn("_ingest_timestamp", lit(datetime(2026, 10, 13, 2, 0)))
        fact_df, quar_df = transform_fact_order(
            bronze_orders, dim_dfs["customers"], dim_dfs["products"], dim_dfs["warehouses"], batch_id="b_test"
        )
        cls.fact_orders = fact_df.collect()
        cls.order_quarantine = quar_df.collect()

        # WMS nightly inventory snapshots against the same warehouse/product dims
        cls.inventory_manifest = []
        cls.inventory_rows = [r for _, r in generate_inventory_rows(
            keys, INVENTORY_NIGHTS, INVENTORY_SKUS, seed=42, bad_rate=0.02, manifest=cls.inventory_manifest)]
        cls.clean_inventory_keys = {
            (r["warehouse_id"], r["product_id"], r["snapshot_date"])
            for _, r in generate_inventory_rows(keys, INVENTORY_NIGHTS, INVENTORY_SKUS, seed=42, bad_rate=0.0)
        }
        bronze_inventory = cls.spark.createDataFrame(cls.inventory_rows, schema=DATASET_SCHEMAS["inventory"]()) \
            .withColumn("_ingest_timestamp", lit(datetime(2026, 10, 13, 2, 0)))
        fact_inv, quar_inv = transform_fact_inventory_snapshot(
            bronze_inventory, dim_dfs["warehouses"], dim_dfs["products"], batch_id="b_test"
        )
        cls.fact_inventory = fact_inv.collect()
        cls.inventory_quarantine = quar_inv.collect()

        # Shipments for the shipped/delivered clean orders, against carrier/route/warehouse dims
        network = build_network(cls.feeds["carriers"], cls.feeds["warehouses"], cls.feeds["routes"], cls.manifest)
        shippable = shippable_orders(iter(cls.order_lines), {m["order_id"] for m in cls.order_manifest})
        cls.shipment_manifest = []
        cls.shipments = list(generate_shipment_records(shippable, network, seed=42, bad_rate=0.02,
                                                       manifest=cls.shipment_manifest))
        bronze_shipments = cls.spark.createDataFrame(cls.shipments, schema=DATASET_SCHEMAS["shipments"]()) \
            .withColumn("_ingest_timestamp", lit(datetime(2026, 10, 13, 2, 0)))
        fact_shp, quar_shp = transform_fact_shipment(
            bronze_shipments, dim_dfs["carriers"], dim_dfs["routes"], batch_id="b_test",
            df_dim_warehouse=dim_dfs["warehouses"],
        )
        cls.fact_shipments = fact_shp.collect()
        cls.shipment_quarantine = quar_shp.collect()

    @classmethod
    def tearDownClass(cls):
        if PYSPARK_AVAILABLE and hasattr(cls, "spark"):
            cls.spark.stop()

    def setUp(self):
        if not PYSPARK_AVAILABLE:
            self.skipTest("PySpark is required for Silver transformation tests.")

    def test_every_corrupted_record_is_quarantined(self):
        for feed in DIM_TRANSFORMS:
            expected = sum(1 for m in self.manifest if m["feed"] == feed)
            _, quar = self.results[feed]
            self.assertEqual(len(quar), expected, f"{feed}: quarantine rows != injected corruptions")

    def test_duplicates_quarantined_under_duplicate_rule(self):
        for feed in DIM_TRANSFORMS:
            expected = sum(1 for m in self.manifest if m["feed"] == feed and m["failure_mode"] == "duplicate")
            _, quar = self.results[feed]
            got = sum(1 for q in quar if q.failed_rule == "RULE_DUPLICATE_KEY")
            self.assertEqual(got, expected, feed)

    def test_clean_records_all_land_in_dimension_once(self):
        for feed in DIM_TRANSFORMS:
            key = CORRUPTIONS[feed][0]
            damaged = {m["original_key"] for m in self.manifest
                       if m["feed"] == feed and m["failure_mode"] != "duplicate"}
            expected_keys = {r[key] for r in build_master_data(COUNTS, seed=42, bad_rate=0.0)[0][feed]} - damaged
            dim, _ = self.results[feed]
            dim_keys = [getattr(r, key) for r in dim]
            self.assertEqual(len(dim_keys), len(set(dim_keys)), f"{feed}: duplicate keys in dimension")
            self.assertEqual(set(dim_keys), expected_keys, feed)

    def test_quarantine_rows_follow_charter_contract(self):
        for feed in DIM_TRANSFORMS:
            for q in self.results[feed][1]:
                self.assertEqual(q.status, "PENDING")
                self.assertEqual(q.batch_id, "b_test")
                self.assertTrue(q.original_payload.startswith("{"))
                self.assertTrue(q.failed_rule.startswith("RULE_"))
                self.assertIsNotNone(q.quarantine_id)
        all_ids = [q.quarantine_id for feed in DIM_TRANSFORMS for q in self.results[feed][1]]
        self.assertEqual(len(all_ids), len(set(all_ids)), "quarantine_id must be unique")

    def test_carrier_master_attributes_reach_dimension(self):
        dim, _ = self.results["carriers"]
        dhl = next(r for r in dim if r.carrier_id == "CARRIER_DHL")
        self.assertEqual(dhl.carrier_name, "DHL Logistics")
        self.assertIsNotNone(dhl.transport_mode)
        self.assertTrue(90.0 <= dhl.contracted_otif_target <= 98.5)
        self.assertTrue(dhl.is_current)

    def test_product_cost_and_price_reach_dimension(self):
        dim, _ = self.results["products"]
        self.assertTrue(all(r.unit_cost is not None and r.unit_price > r.unit_cost for r in dim))

    def test_each_failure_mode_maps_to_its_specific_rule(self):
        """Every injected failure mode is quarantined under the rule that names it, so the
        DQ dashboard's root-cause breakdown matches what actually went wrong."""
        for feed in DIM_TRANSFORMS:
            expected = Counter(EXPECTED_RULE[(feed, m["failure_mode"])]
                               for m in self.manifest if m["feed"] == feed)
            got = Counter(q.failed_rule for q in self.results[feed][1])
            self.assertEqual(got, expected, feed)

    def test_payload_keeps_null_fields(self):
        """A null-key record's payload must show the key as null, not omit it."""
        _, quar = self.results["warehouses"]
        missing = [q for q in quar if q.failed_rule == "RULE_WAREHOUSE_ID_MISSING"]
        self.assertTrue(missing)
        for q in missing:
            payload = json.loads(q.original_payload)
            self.assertIn("warehouse_id", payload)
            self.assertIsNone(payload["warehouse_id"])

    def test_failure_reason_includes_offending_value(self):
        _, quar = self.results["products"]
        weight = next(q for q in quar if q.failed_rule == "RULE_PRODUCT_WEIGHT_NON_POSITIVE")
        bad_value = json.loads(weight.original_payload)["unit_weight_kg"]
        self.assertIn(str(bad_value), weight.failure_reason)


    # -------------------------------------------------------------------- fact_order

    def test_every_corrupted_order_line_is_quarantined_under_its_rule(self):
        expected = Counter(EXPECTED_ORDER_RULE[m["failure_mode"]] for m in self.order_manifest)
        got = Counter(q.failed_rule for q in self.order_quarantine)
        self.assertEqual(got, expected)

    def test_clean_order_lines_all_land_once(self):
        damaged = {(m["order_id"], m["order_line_number"]) for m in self.order_manifest
                   if m["failure_mode"] != "duplicate"}
        # null_order_id lines are logged under their original ID, so drop null keys explicitly
        expected = {(l["order_id"], l["order_line_number"]) for l in self.order_lines
                    if l["order_id"] is not None} - damaged
        got = [(r.order_id, r.order_line_number) for r in self.fact_orders]
        # Conservation: every Bronze line is either in the fact, quarantined, or a superseded copy
        self.assertEqual(len(self.order_lines), len(got) + len(self.order_quarantine))
        self.assertEqual(len(got), len(set(got)), "duplicate order lines in fact_order")
        self.assertEqual(set(got), expected)

    def test_fact_order_keys_and_measures(self):
        order_dates = {(l["order_id"], l["order_line_number"]): l["order_date"] for l in self.order_lines}
        for r in self.fact_orders[:200]:
            self.assertIsNotNone(r.customer_sk)
            self.assertIsNotNone(r.product_sk)
            self.assertIsNotNone(r.origin_warehouse_sk)
            expected_sk = int(order_dates[(r.order_id, r.order_line_number)].replace("-", ""))
            self.assertEqual(r.order_date_sk, expected_sk)
            self.assertTrue(0 <= r.shipped_quantity <= r.ordered_quantity)
            self.assertAlmostEqual(r.total_price, r.ordered_quantity * r.unit_price, places=6)
            self.assertIsNotNone(r.updated_at)
        statuses = {r.order_status for r in self.fact_orders}
        self.assertEqual(statuses, {"CREATED", "PICKED", "SHIPPED", "DELIVERED", "CANCELLED"})

    def test_order_quarantine_payload_is_the_bronze_record(self):
        q = next(q for q in self.order_quarantine if q.failed_rule == "RULE_REF_INTEGRITY_WAREHOUSE")
        payload = json.loads(q.original_payload)
        self.assertEqual(payload["warehouse_id"], "WH-9999")
        for working_col in ("customer_sk", "product_sk", "origin_warehouse_sk", "_grain_key", "_order_ts"):
            self.assertNotIn(working_col, payload)

    def test_newer_order_line_version_supersedes_older_without_quarantine(self):
        """CDC-style updates: same line, later updated_at -> newest wins, older is history, not bad data."""
        damaged = {(m["order_id"], m["order_line_number"]) for m in self.order_manifest}
        base = next(l for l in self.order_lines
                    if l["order_status"] == "SHIPPED" and (l["order_id"], l["order_line_number"]) not in damaged)
        older = dict(base, order_status="PICKED", shipped_quantity=0, updated_at="2026-01-01T00:00:00Z")
        newer = dict(base, order_status="DELIVERED", updated_at="2026-10-11T00:00:00Z")
        df = self.spark.createDataFrame([older, newer], schema=DATASET_SCHEMAS["orders"]())
        fact, quar = transform_fact_order(
            df, self.dim_dfs["customers"], self.dim_dfs["products"], self.dim_dfs["warehouses"])
        rows = fact.collect()
        self.assertEqual(quar.count(), 0, [q.failed_rule for q in quar.collect()])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].order_status, "DELIVERED")


    # -------------------------------------------------------------- fact_inventory_snapshot

    def test_every_corrupted_inventory_row_is_quarantined_under_its_rule(self):
        expected = Counter(EXPECTED_INVENTORY_RULE[m["failure_mode"]] for m in self.inventory_manifest)
        got = Counter(q.failed_rule for q in self.inventory_quarantine)
        self.assertEqual(got, expected)

    def test_clean_inventory_rows_all_land_once(self):
        damaged = {(m["warehouse_id"], m["product_id"], m["snapshot_date"]) for m in self.inventory_manifest
                   if m["failure_mode"] != "duplicate"}
        expected = self.clean_inventory_keys - damaged
        got = [(r.warehouse_id, r.product_id, r.snapshot_date.isoformat()) for r in self.fact_inventory]
        self.assertEqual(len(got), len(set(got)), "duplicate snapshot rows in fact")
        self.assertEqual(set(got), expected)
        # Conservation: every Bronze row is in the fact or quarantined
        self.assertEqual(len(self.inventory_rows), len(got) + len(self.inventory_quarantine))

    def test_inventory_measures_and_flags(self):
        stockouts = 0
        for r in self.fact_inventory:
            self.assertIsNotNone(r.warehouse_sk)
            self.assertIsNotNone(r.product_sk)
            self.assertEqual(r.quantity_available, r.quantity_on_hand - r.quantity_reserved)
            self.assertEqual(r.is_stockout, r.quantity_available <= 0)
            self.assertEqual(r.is_stockout_risk, r.quantity_available <= r.safety_stock)
            self.assertEqual(r.snapshot_date_sk, int(r.snapshot_date.strftime("%Y%m%d")))
            self.assertIsNotNone(r.recorded_at)
            stockouts += r.is_stockout
        self.assertGreater(stockouts, 0, "generator should produce some genuine stockouts")

    def test_resent_inventory_file_supersedes_older_snapshot(self):
        """A corrected re-export (later recorded_at) replaces the earlier snapshot; not bad data."""
        damaged = {(m["warehouse_id"], m["product_id"], m["snapshot_date"]) for m in self.inventory_manifest}
        base = next(r for r in self.inventory_rows
                    if (r["warehouse_id"], r["product_id"], r["snapshot_date"]) not in damaged)
        first = dict(base, quantity_on_hand=100, quantity_reserved=0, recorded_at=base["snapshot_date"] + "T01:00:00Z")
        resent = dict(base, quantity_on_hand=80, quantity_reserved=0, recorded_at=base["snapshot_date"] + "T06:30:00Z")
        df = self.spark.createDataFrame([first, resent], schema=DATASET_SCHEMAS["inventory"]())
        fact, quar = transform_fact_inventory_snapshot(df, self.dim_dfs["warehouses"], self.dim_dfs["products"])
        rows = fact.collect()
        self.assertEqual(quar.count(), 0, [q.failed_rule for q in quar.collect()])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].quantity_on_hand, 80.0)


    # -------------------------------------------------------------------- fact_shipment

    def test_every_corrupted_shipment_is_quarantined_under_its_rule(self):
        expected = Counter(EXPECTED_SHIPMENT_RULE[m["failure_mode"]] for m in self.shipment_manifest)
        got = Counter(q.failed_rule for q in self.shipment_quarantine)
        self.assertEqual(got, expected)

    def test_clean_shipments_all_land_once(self):
        damaged = {m["shipment_id"] for m in self.shipment_manifest if m["failure_mode"] != "duplicate"}
        expected = {s["shipment_id"] for s in self.shipments if s["shipment_id"] is not None} - damaged
        got = [r.shipment_id for r in self.fact_shipments]
        self.assertEqual(len(got), len(set(got)))
        self.assertEqual(set(got), expected)
        self.assertEqual(len(self.shipments), len(got) + len(self.shipment_quarantine))

    def test_shipment_dimension_keys_resolve(self):
        for r in self.fact_shipments:
            self.assertIsNotNone(r.carrier_sk)
            self.assertIsNotNone(r.route_sk)
            self.assertIsNotNone(r.origin_warehouse_sk, "origin warehouse link must now be populated")
            self.assertIsNotNone(r.order_id)

    def test_otif_columns(self):
        delivered = [r for r in self.fact_shipments if r.shipment_status == "DELIVERED"]
        in_transit = [r for r in self.fact_shipments if r.shipment_status == "IN_TRANSIT"]
        self.assertTrue(delivered and in_transit)
        for r in delivered:
            self.assertIsNotNone(r.actual_delivery_time)
            self.assertEqual(r.is_on_time, r.actual_delivery_time <= r.planned_delivery_time)
            expected_delay = int((r.actual_delivery_time - r.planned_delivery_time).total_seconds() // 60)
            self.assertLessEqual(abs(r.delay_minutes - expected_delay), 1)
            self.assertEqual(r.is_in_full, r.shipped_quantity >= r.ordered_quantity)
            self.assertGreater(r.actual_transit_hours, 0)
        for r in in_transit:
            self.assertIsNone(r.is_on_time)
            self.assertIsNone(r.delay_minutes)
        otif = sum(1 for r in delivered if r.is_on_time and r.is_in_full) / len(delivered)
        self.assertTrue(0.65 < otif < 0.95, otif)

    def test_delivery_update_supersedes_in_transit_version(self):
        """IN_TRANSIT then DELIVERED for the same shipment is an update, not a duplicate."""
        damaged = {m["shipment_id"] for m in self.shipment_manifest}
        base = next(s for s in self.shipments
                    if s["shipment_status"] == "DELIVERED" and s["shipment_id"] not in damaged)
        in_transit = dict(base, shipment_status="IN_TRANSIT", actual_delivery_time_utc=None,
                          updated_at=base["dispatch_time_utc"])
        df = self.spark.createDataFrame([in_transit, base], schema=DATASET_SCHEMAS["shipments"]())
        fact, quar = transform_fact_shipment(df, self.dim_dfs["carriers"], self.dim_dfs["routes"],
                                             df_dim_warehouse=self.dim_dfs["warehouses"])
        rows = fact.collect()
        self.assertEqual(quar.count(), 0, [q.failed_rule for q in quar.collect()])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].shipment_status, "DELIVERED")


if __name__ == "__main__":
    unittest.main()
