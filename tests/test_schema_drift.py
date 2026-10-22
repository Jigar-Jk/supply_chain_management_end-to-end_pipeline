"""
test_schema_drift.py – Files that no longer match their feed's contract (failure scenario 3).

Scenario: the WMS export renames quantity_on_hand to qty_on_hand. Auto Loader (rescue mode) leaves
quantity_on_hand NULL and keeps {"qty_on_hand": ...} in _rescued_data. Silver must quarantine those
rows naming the real cause (RULE_SCHEMA_DRIFT, not "quantity missing"), report the whole file as
rejected, and accept the new name only once an alias is explicitly configured.

Run locally with:
    python -m unittest tests.test_schema_drift -v
"""

import json
import unittest
from datetime import date, datetime
from unittest.mock import patch

from data_generators.generate_inventory import generate_inventory_rows, snapshot_nights
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import usable_master_keys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform import transform_silver as ts
    from scm_platform.ingest_bronze import DATASET_SCHEMAS

COUNTS = {"warehouses": 6, "carriers": 3, "products": 60, "customers": 20, "routes": 10}
NIGHT = date(2026, 10, 21)
LANDING = "abfss://landing@stscmplatformjk1.dfs.core.windows.net/inventory/"


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestSchemaDrift(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestSchemaDrift")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        feeds, manifest = build_master_data(COUNTS, seed=42, bad_rate=0.0)
        keys = usable_master_keys(feeds, manifest)
        cls.warehouses = sorted(keys["warehouses"])
        rows = [r for _, r in generate_inventory_rows(keys, snapshot_nights(NIGHT, 1), 8, seed=42, bad_rate=0.0)]
        loaded = F.lit(datetime(2026, 10, 21, 1, 30))
        # One file per warehouse; the first warehouse's file has the renamed column (every row drifts),
        # the second has a single drifted row (bad rows, not a format change).
        drifted_file, partly_file = cls.warehouses[0], cls.warehouses[1]
        is_drifted = (F.col("warehouse_id") == drifted_file) | (
            (F.col("warehouse_id") == partly_file)
            & (F.col("product_id") == F.min("product_id").over(Window.partitionBy("warehouse_id"))))
        source_file = F.concat(F.lit(LANDING), F.col("warehouse_id"), F.lit(".jsonl"))
        cls.bronze = (
            spark.createDataFrame(rows, schema=DATASET_SCHEMAS["inventory"]())
            .withColumn("_ingest_timestamp", loaded)
            .withColumn("_source_file", source_file)
            .withColumn("_batch_id", F.lit("run-1"))
            .withColumn("_drift", is_drifted)
            .withColumn("_rescued_data", F.when(F.col("_drift"), F.to_json(F.create_map(
                F.lit("qty_on_hand"), F.col("quantity_on_hand").cast("string"),
                F.lit("_file_path"), source_file))))
            .withColumn("quantity_on_hand", F.when(F.col("_drift"), F.lit(None)).otherwise(F.col("quantity_on_hand")))
            .drop("_drift")
        )
        cls.bronze = spark.createDataFrame(cls.bronze.collect(), cls.bronze.schema).cache()   # flat plan
        dim_wh = ts.transform_dim_warehouse(
            spark.createDataFrame(feeds["warehouses"], schema=DATASET_SCHEMAS["warehouses"]()).withColumn("_ingest_timestamp", loaded))[0]
        dim_p = ts.transform_dim_product(
            spark.createDataFrame(feeds["products"], schema=DATASET_SCHEMAS["products"]()).withColumn("_ingest_timestamp", loaded))[0]
        cls.dim_wh, cls.dim_p = dim_wh.cache(), dim_p.cache()
        cls.fact, quar = ts.transform_fact_inventory_snapshot(cls.bronze, cls.dim_wh, cls.dim_p, batch_id="run-1")
        cls.quarantine = quar.collect()
        cls.drifted_file, cls.partly_file = drifted_file, partly_file

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ row level

    def test_renamed_column_is_quarantined_with_the_real_cause(self):
        drift = [q for q in self.quarantine if q.failed_rule == "RULE_SCHEMA_DRIFT"]
        expected = self.bronze.filter(F.col("_rescued_data").isNotNull()).count()
        self.assertEqual(len(drift), expected)
        for q in drift:
            self.assertIn("qty_on_hand", q.failure_reason)
            self.assertNotIn("_file_path", q.failure_reason)        # Auto Loader's bookkeeping, not drift

    def test_drift_wins_over_the_symptom(self):
        """quantity_on_hand is NULL on those rows too, but the cause (drift) must be what is recorded."""
        self.assertFalse(any(q.failed_rule != "RULE_SCHEMA_DRIFT" for q in self.quarantine))

    def test_quarantine_keeps_the_rescued_value(self):
        payload = json.loads(next(q for q in self.quarantine if q.failed_rule == "RULE_SCHEMA_DRIFT").original_payload)
        self.assertIn("qty_on_hand", json.loads(payload["_rescued_data"]))   # nothing lost: a steward can fix it

    def test_clean_rows_reach_silver(self):
        clean = self.bronze.filter(F.col("_rescued_data").isNull()).count()
        self.assertEqual(self.fact.count(), clean)

    def test_file_path_alone_is_not_drift(self):
        only_path = self.bronze.filter(F.col("_rescued_data").isNull()).limit(3) \
            .withColumn("_rescued_data", F.to_json(F.create_map(F.lit("_file_path"), F.col("_source_file"))))
        _, quar = ts.transform_fact_inventory_snapshot(only_path, self.dim_wh, self.dim_p, batch_id="run-1")
        self.assertEqual(quar.count(), 0)

    # ------------------------------------------------------------------ file level

    def test_report_rejects_only_the_reformatted_file(self):
        report = {r.source_file: r for r in ts.schema_drift_report(self.bronze).collect()}
        drifted = report[f"{LANDING}{self.drifted_file}.jsonl"]
        partly = report[f"{LANDING}{self.partly_file}.jsonl"]
        self.assertTrue(drifted.rejected)
        self.assertEqual(drifted.rows_drifted, drifted.rows_read)
        self.assertEqual(list(drifted.unexpected_fields), ["qty_on_hand"])
        self.assertFalse(partly.rejected)                            # one bad row is not a format change
        self.assertEqual(partly.rows_drifted, 1)
        self.assertEqual(len(report), 2)                             # clean files are not reported

    def test_rejected_file_becomes_a_run_log_row(self):
        rows = ts.rejected_file_log_rows(ts.schema_drift_report(self.bronze), "inventory", "run-1").collect()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r.run_id, r.source_system, r.status, r.layer), ("run-1", "wms_batch", "REJECTED_FILE", "silver"))
        self.assertTrue(r.step.startswith("schema_check:inventory:") and r.step.endswith(f"{self.drifted_file}.jsonl"))
        self.assertIn("qty_on_hand", r.error_message)
        self.assertEqual(r.rows_quarantined, r.rows_read)

    # ------------------------------------------------------------------ agreed rename

    def test_no_alias_by_default(self):
        self.assertEqual(ts.COLUMN_ALIASES, {})
        self.assertIs(ts.conform_to_contract(self.bronze, "inventory"), self.bronze)

    def test_agreed_alias_heals_the_rows(self):
        with patch.dict(ts.COLUMN_ALIASES, {"inventory": {"qty_on_hand": "quantity_on_hand"}}):
            conformed = ts.conform_to_contract(self.bronze, "inventory")
            self.assertEqual(conformed.filter(F.col("quantity_on_hand").isNull()).count(), 0)
            _, quar = ts.transform_fact_inventory_snapshot(conformed, self.dim_wh, self.dim_p, batch_id="run-1")
            self.assertEqual(quar.count(), 0)
            self.assertEqual(ts.schema_drift_report(conformed).count(), 0)

    # ------------------------------------------------------------------ wiring

    def test_drift_rule_runs_first_for_every_file_feed(self):
        rule_sets = [ts.ROUTE_RULES, ts.CARRIER_RULES, ts.CUSTOMER_RULES, ts.PRODUCT_RULES, ts.WAREHOUSE_RULES,
                     ts.order_rules(True), ts.shipment_rules(True), ts.inventory_rules()]
        for rules in rule_sets:
            self.assertIs(rules[0], ts.SCHEMA_DRIFT_RULE)

    def test_cdc_and_gps_rules_have_no_drift_rule(self):
        """CDC and GPS rows do not come from Auto Loader files, so they have no _rescued_data."""
        self.assertNotIn(ts.SCHEMA_DRIFT_RULE, ts.inventory_current_rules())
        self.assertNotIn(ts.SCHEMA_DRIFT_RULE, ts.gps_rules())


if __name__ == "__main__":
    unittest.main()
