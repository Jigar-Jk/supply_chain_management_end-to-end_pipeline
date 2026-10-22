"""
test_quarantine_alert.py – The 5 % quarantine-rate early warning (dashboards/alerts/quarantine_rate_alert.sql).

The deployed SQL runs on local Spark against temp views (localize_sql). Today's data:

    feed        processed   quarantined        expected
    shipments        100    25 (RULE_C)        CRITICAL 25 %
    orders           100     8 (6 A + 2 B)     WARNING  8 %, top rule RULE_A
    inventory        200     4                 nothing (2 %)
    customers         20     5                 nothing (25 %, but under 100 rows: noise)
    routes           100    50, 2 days ago     nothing (not today)

Run locally with:
    python -m unittest tests.test_quarantine_alert -v
"""

import re
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

ROOT = Path(__file__).resolve().parent.parent
ALERT_SQL = ROOT / "dashboards" / "alerts" / "quarantine_rate_alert.sql"
ALERTS_TF = ROOT / "infra" / "alerts.tf"
BRONZE_FEEDS = ["orders", "orders_cdc", "inventory", "shipments", "warehouses", "products", "customers", "carriers",
                "routes", "gps_events"]
PROCESSED_TODAY = {"orders": 100, "inventory": 200, "shipments": 100, "customers": 20}


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestQuarantineRateAlert(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestQuarantineAlert")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        # "now" exactly, so a test run just after midnight cannot drift into yesterday
        cls.now = F.current_timestamp()
        two_days_ago = F.current_timestamp() - F.expr("INTERVAL 2 DAYS")
        for feed in BRONZE_FEEDS:
            ts = two_days_ago if feed == "routes" else cls.now
            n = 100 if feed == "routes" else PROCESSED_TODAY.get(feed, 0)
            spark.range(n).select(ts.alias("_ingest_timestamp")).createOrReplaceTempView(local_name("bronze", feed))
        cls.quarantine_rows = (
            [("shipments", "RULE_C", 0)] * 25
            + [("orders", "RULE_A", 0)] * 6 + [("orders", "RULE_B", 0)] * 2
            + [("inventory", "RULE_D", 0)] * 4
            + [("customers", "RULE_E", 0)] * 5
            + [("routes", "RULE_F", 2)] * 50                           # 2 days old
        )
        cls.register_quarantine(cls.quarantine_rows)
        cls.sql = localize_sql(ALERT_SQL.read_text(encoding="utf-8"))

    @classmethod
    def register_quarantine(cls, rows):
        df = cls.spark.createDataFrame(rows or [("none", "none", 0)], "source_feed string, failed_rule string, days_ago int")
        if not rows:
            df = df.limit(0)
        df.withColumn("ingested_at", F.current_timestamp() - F.expr("make_interval(0, 0, 0, days_ago)")) \
            .createOrReplaceTempView(local_name("quarantine", "records"))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def alert(self):
        return self.spark.sql(self.sql).collect()

    # ------------------------------------------------------------------ what fires

    def test_only_breaching_feeds_worst_first(self):
        rows = self.alert()
        self.assertEqual([(r.source_feed, r.severity, float(r.quarantine_pct)) for r in rows],
                         [("shipments", "CRITICAL", 25.0), ("orders", "WARNING", 8.0)])

    def test_names_the_top_failing_rule(self):
        orders = next(r for r in self.alert() if r.source_feed == "orders")
        self.assertEqual((orders.top_rule, orders.top_rule_rows, orders.rows_quarantined, orders.rows_processed),
                         ("RULE_A", 6, 8, 100))

    # ------------------------------------------------------------------ what stays quiet

    def test_normal_rate_small_feeds_and_old_days_are_quiet(self):
        fired = {r.source_feed for r in self.alert()}
        self.assertNotIn("inventory", fired)    # 2 %: normal
        self.assertNotIn("customers", fired)    # 25 % of 20 rows: under min_rows
        self.assertNotIn("routes", fired)       # 50 % but two days ago

    def test_clean_day_returns_no_rows(self):
        try:
            self.register_quarantine([])
            self.assertEqual(self.alert(), [])                     # empty result = alert OK
        finally:
            self.register_quarantine(self.quarantine_rows)

    def test_exactly_five_percent_fires(self):
        try:
            self.register_quarantine([("orders", "RULE_A", 0)] * 5)
            rows = self.alert()
            self.assertEqual([(r.source_feed, float(r.quarantine_pct), r.severity) for r in rows], [("orders", 5.0, "WARNING")])
        finally:
            self.register_quarantine(self.quarantine_rows)

    # ------------------------------------------------------------------ deployed definitions agree

    def test_terraform_threshold_matches_the_sql(self):
        """The alert checks the first row's quarantine_pct >= 5; the SQL must use the same 5 %."""
        sql_warn = float(re.search(r"([\d.]+)\s+AS\s+warn_pct", ALERT_SQL.read_text(encoding="utf-8")).group(1))
        tf = ALERTS_TF.read_text(encoding="utf-8")
        self.assertEqual(float(re.search(r"double_value\s*=\s*([\d.]+)", tf).group(1)), sql_warn)
        self.assertIn('name = "quarantine_pct"', tf)
        self.assertIn('empty_result_state = "OK"', tf)

    def test_daily_job_evaluates_the_alert_even_after_a_failure(self):
        job = (ROOT / "databricks" / "resources" / "scm_daily_pipeline.yml").read_text(encoding="utf-8")
        task = job[job.index("task_key: quarantine_rate_alert"):]
        self.assertIn("run_if: ALL_DONE", task)
        self.assertIn("alert_id: ${var.quarantine_alert_id}", task)


if __name__ == "__main__":
    unittest.main()
