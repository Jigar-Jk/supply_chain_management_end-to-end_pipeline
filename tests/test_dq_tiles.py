"""
test_dq_tiles.py – The 4 data-quality dashboard tiles (dashboards/dq/*.sql), on hand-built data.

The SQL under test is the deployed tile SQL, localized mechanically (local/preview/sql_local.py).
Timestamps are built from UTC text inside Spark, so the session time zone cannot shift them.

Run locally with:
    python -m unittest tests.test_dq_tiles -v
"""

import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from local.preview.sql_local import local_name, localize_sql

DQ_DIR = Path(__file__).resolve().parent.parent / "dashboards" / "dq"
BRONZE_FEEDS = ["orders", "inventory", "shipments", "warehouses", "products", "customers", "carriers", "routes",
                "gps_events"]


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestDqTiles(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestDqTiles")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        minutes_ago = lambda m: F.current_timestamp() - F.expr(f"INTERVAL {m} MINUTES")  # noqa: E731

        # Bronze: 10 orders and 20 inventory rows today; the other feeds empty
        for feed in BRONZE_FEEDS:
            n = {"orders": 10, "inventory": 20}.get(feed, 0)
            spark.range(n).select(minutes_ago(30).alias("_ingest_timestamp")) \
                .createOrReplaceTempView(local_name("bronze", feed))

        # Quarantine: orders 2 PENDING + 1 REPROCESSED + 1 REJECTED; inventory 1 REPROCESSED
        rows = [("q1", "orders", "RULE_A", "PENDING", 0), ("q2", "orders", "RULE_A", "PENDING", 1),
                ("q3", "orders", "RULE_B", "REPROCESSED", 1), ("q4", "orders", "RULE_C", "REJECTED", 2),
                ("q5", "inventory", "RULE_D", "REPROCESSED", 1)]
        spark.createDataFrame(rows, "quarantine_id string, source_feed string, failed_rule string, status string, "
                                    "retry_count int") \
            .withColumn("ingested_at", minutes_ago(20)).createOrReplaceTempView(local_name("quarantine", "records"))

        # SLA: wms has a recent SLA miss (RED), carrier is 90 min stale vs 60 (AMBER),
        # CDC ran 5 min ago vs 15 (GREEN), GPS never succeeded (RED)
        spark.createDataFrame([("wms_batch", "02:00", 120), ("carrier_api", "01:00", 60), ("sqlserver_cdc", None, 15),
                               ("gps_stream", None, 5)], "source_system string, expected_by_time string, max_lag_minutes int") \
            .createOrReplaceTempView(local_name("ops", "sla_config"))
        log = [("wms_batch", "SUCCESS", 600, None), ("wms_batch", "SLA_MISS", 60, "WMS snapshot missing"),
               ("carrier_api", "SUCCESS", 90, None), ("sqlserver_cdc", "SUCCESS", 5, None)]
        spark.createDataFrame(log, "source_system string, status string, mins int, error_message string") \
            .withColumn("ended_at", F.expr("current_timestamp() - make_interval(0, 0, 0, 0, 0, mins, 0)")) \
            .createOrReplaceTempView(local_name("ops", "pipeline_run_log"))

        cls.tiles = {p.stem: spark.sql(localize_sql(p.read_text(encoding="utf-8"))).collect()
                     for p in sorted(DQ_DIR.glob("*.sql"))}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_all_four_tiles_exist(self):
        self.assertEqual(sorted(self.tiles), ["01_volume_vs_quarantined", "02_quarantine_by_rule",
                                              "03_reprocessing_success_rate", "04_pipeline_sla_status"])

    def test_tile1_volume_vs_quarantined(self):
        got = {r.source_feed: (r.rows_processed, r.rows_quarantined, float(r.quarantine_pct))
               for r in self.tiles["01_volume_vs_quarantined"]}
        self.assertEqual(got, {"orders": (10, 4, 40.0), "inventory": (20, 1, 5.0)})

    def test_tile2_quarantine_by_rule(self):
        got = {(r.source_feed, r.failed_rule, r.status): (r.rows, r.max_retries) for r in self.tiles["02_quarantine_by_rule"]}
        self.assertEqual(got[("orders", "RULE_A", "PENDING")], (2, 1))
        self.assertEqual(len(got), 4)

    def test_tile3_reprocessing_success_rate(self):
        got = {r.source_feed: (r.reprocessed, r.rejected, r.still_pending, r.reprocessing_success_pct)
               for r in self.tiles["03_reprocessing_success_rate"]}
        self.assertEqual(got["orders"][:3], (1, 1, 2))
        self.assertAlmostEqual(float(got["orders"][3]), 50.0)
        self.assertAlmostEqual(float(got["inventory"][3]), 100.0)
        self.assertEqual(got["ALL"][:3], (2, 1, 2))
        self.assertAlmostEqual(float(got["ALL"][3]), 66.67, places=2)

    def test_tile4_pipeline_sla_status(self):
        got = {r.source_system: r.sla_status for r in self.tiles["04_pipeline_sla_status"]}
        self.assertEqual(got, {"wms_batch": "RED", "carrier_api": "AMBER", "sqlserver_cdc": "GREEN", "gps_stream": "RED"})
        wms = next(r for r in self.tiles["04_pipeline_sla_status"] if r.source_system == "wms_batch")
        self.assertEqual(wms.miss_message, "WMS snapshot missing")

    def test_tiles_read_only_bronze_quarantine_and_ops(self):
        import re
        for p in DQ_DIR.glob("*.sql"):
            for ref in re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", p.read_text(encoding="utf-8"), flags=re.IGNORECASE):
                if "." in ref:
                    self.assertIn(ref.split(".")[0], ("bronze", "quarantine", "ops"), (p.name, ref))


if __name__ == "__main__":
    unittest.main()
