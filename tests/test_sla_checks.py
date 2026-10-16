"""
test_sla_checks.py – WMS nightly file SLA and the last-known-good fallback.

Run locally with:
    python -m unittest tests.test_sla_checks -v
"""

import re
import unittest
from datetime import date, datetime
from pathlib import Path

from data_generators.generate_inventory import generate_inventory_rows, snapshot_nights
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import usable_master_keys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    from scm_platform import transform_silver as ts
    from scm_platform.sla_checks import (
        LATE, ON_TIME, SLA_MISS, WAITING, carry_forward_last_good, check_wms_snapshot, run_log_row,
    )

def utc(text_col):
    """A UTC timestamp column from 'yyyy-MM-dd HH:mm:ss' text, independent of the session time zone."""
    return F.to_utc_timestamp(text_col.cast("timestamp"), F.current_timezone())


COUNTS = {"warehouses": 12, "carriers": 5, "products": 120, "customers": 50, "routes": 40}
NIGHTS = snapshot_nights(date(2026, 10, 12), 2)           # D-1 = 10-11, D = 10-12
D_MINUS_1, D = NIGHTS
STOCKOUT_SQL = Path(__file__).resolve().parent.parent / "liquibase" / "changelog" / "gold" / "03_vw_stockout_rate_daily.sql"


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestWmsSla(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestSla")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        feeds, manifest = build_master_data(COUNTS, seed=42, bad_rate=0.0)
        keys = usable_master_keys(feeds, manifest)
        cls.warehouses = keys["warehouses"]
        rows = [r for _, r in generate_inventory_rows(keys, NIGHTS, 15, seed=42, bad_rate=0.0)]
        # Each night's file lands at 01:30 UTC (built from UTC strings, never from local-time datetimes)
        cls.bronze_all = spark.createDataFrame(rows, schema=DATASET_SCHEMAS["inventory"]()) \
            .withColumn("_ingest_timestamp", utc(F.concat(F.col("snapshot_date"), F.lit(" 01:30:00")))).cache()
        cls.bronze_missing_d = cls.bronze_all.filter(F.col("snapshot_date") != D.isoformat()).cache()
        lit_ts = F.lit(datetime(2026, 10, 11, 2, 0))
        cls.dim_wh = ts.transform_dim_warehouse(spark.createDataFrame(feeds["warehouses"], schema=DATASET_SCHEMAS["warehouses"]())
                                                .withColumn("_ingest_timestamp", lit_ts))[0].cache()
        dim_p = ts.transform_dim_product(spark.createDataFrame(feeds["products"], schema=DATASET_SCHEMAS["products"]())
                                         .withColumn("_ingest_timestamp", lit_ts))[0]
        cls.fact_all = ts.transform_fact_inventory_snapshot(cls.bronze_all, cls.dim_wh, dim_p)[0].cache()
        cls.fact_missing_d = cls.fact_all.filter(F.col("snapshot_date") != F.lit(D)).cache()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ arrival status

    def test_missing_after_deadline_is_sla_miss(self):
        s = check_wms_snapshot(self.bronze_missing_d, self.warehouses, D, now=datetime(2026, 10, 12, 3, 0))
        self.assertEqual(s.status, SLA_MISS)
        self.assertEqual(s.missing_warehouses, sorted(self.warehouses))
        self.assertIn("02:00 UTC deadline", s.message)

    def test_missing_before_deadline_is_waiting(self):
        s = check_wms_snapshot(self.bronze_missing_d, self.warehouses, D, now=datetime(2026, 10, 12, 1, 0))
        self.assertEqual(s.status, WAITING)

    def test_arrived_before_deadline_is_on_time(self):
        s = check_wms_snapshot(self.bronze_all, self.warehouses, D, now=datetime(2026, 10, 12, 3, 0))
        self.assertEqual(s.status, ON_TIME)
        self.assertEqual(s.missing_warehouses, [])

    def test_arrived_after_deadline_is_late(self):
        late = self.bronze_all.withColumn("_ingest_timestamp", utc(F.lit("2026-10-12 05:00:00")))
        s = check_wms_snapshot(late, self.warehouses, D, now=datetime(2026, 10, 12, 6, 0))
        self.assertEqual(s.status, LATE)

    def test_one_missing_warehouse_is_detected(self):
        gone = self.warehouses[0]
        partial = self.bronze_all.filter(~((F.col("snapshot_date") == D.isoformat()) & (F.col("warehouse_id") == gone)))
        s = check_wms_snapshot(partial, self.warehouses, D, now=datetime(2026, 10, 12, 3, 0))
        self.assertEqual(s.status, SLA_MISS)
        self.assertEqual(s.missing_warehouses, [gone])
        self.assertEqual(sum(v == ON_TIME for v in s.by_warehouse.values()), len(self.warehouses) - 1)

    def test_run_log_row_status(self):
        miss = check_wms_snapshot(self.bronze_missing_d, self.warehouses, D, now=datetime(2026, 10, 12, 3, 0))
        ok = check_wms_snapshot(self.bronze_all, self.warehouses, D, now=datetime(2026, 10, 12, 3, 0))
        self.assertEqual(run_log_row(miss, "r1", datetime(2026, 10, 12, 3, 0), 10)[9], "SLA_MISS")
        self.assertEqual(run_log_row(ok, "r1", datetime(2026, 10, 12, 3, 0), 0)[9], "SUCCESS")

    # ------------------------------------------------------------------ last-known-good

    def test_carry_forward_copies_last_good_night_as_stale(self):
        stale = carry_forward_last_good(self.fact_missing_d, D, self.warehouses).cache()
        previous = self.fact_missing_d.filter(F.col("snapshot_date") == F.lit(D_MINUS_1))
        self.assertEqual(stale.count(), previous.count())
        self.assertEqual(stale.filter(~F.col("is_stale")).count(), 0)
        self.assertEqual({r.snapshot_date for r in stale.select("snapshot_date").distinct().collect()}, {D})
        self.assertEqual(stale.select("snapshot_date_sk").distinct().first()[0], 20261012)
        # Same stock as the night before, new keys for night D
        joined = stale.alias("s").join(previous.alias("p"), ["warehouse_id", "product_id"])
        self.assertEqual(joined.filter("s.quantity_on_hand <> p.quantity_on_hand").count(), 0)
        self.assertEqual(joined.filter("s.inventory_snapshot_sk = p.inventory_snapshot_sk").count(), 0)
        # Keys match what the real night-D rows will have, so the late file's MERGE replaces them
        real_d = self.fact_all.filter(F.col("snapshot_date") == F.lit(D)).select("inventory_snapshot_sk")
        self.assertEqual(stale.select("inventory_snapshot_sk").subtract(real_d).count(), 0)

    def test_late_real_file_is_newer_than_the_stale_copy(self):
        stale = carry_forward_last_good(self.fact_missing_d, D, self.warehouses)
        real = self.fact_all.filter(F.col("snapshot_date") == F.lit(D))
        pairs = stale.alias("s").join(real.alias("r"), "inventory_snapshot_sk")
        self.assertEqual(pairs.filter("NOT (r.recorded_at > s.recorded_at)").count(), 0,
                         "MERGE (newer recorded_at wins) must let the real snapshot replace the stale one")
        self.assertEqual(pairs.filter("r.is_stale").count(), 0)

    def test_only_missing_warehouses_are_carried(self):
        stale = carry_forward_last_good(self.fact_missing_d, D, self.warehouses[:1])
        self.assertEqual({r.warehouse_id for r in stale.select("warehouse_id").distinct().collect()},
                         {self.warehouses[0]})
        self.assertEqual(carry_forward_last_good(self.fact_missing_d, D, []).count(), 0)

    def test_gold_reports_built_from_stale_data(self):
        stale = carry_forward_last_good(self.fact_missing_d, D, self.warehouses)
        self.fact_missing_d.unionByName(stale).createOrReplaceTempView("fact_inventory_snapshot")
        self.dim_wh.createOrReplaceTempView("dim_warehouse")
        sql = re.sub(r"CREATE OR REPLACE VIEW", "CREATE OR REPLACE TEMP VIEW", STOCKOUT_SQL.read_text(encoding="utf-8"))
        self.spark.sql(re.sub(r"\b(silver|gold)\.", "", sql))
        flags = {(r.kpi_date, r.built_from_stale_data) for r in
                 self.spark.table("vw_stockout_rate_daily").select("kpi_date", "built_from_stale_data").distinct().collect()}
        self.assertEqual(flags, {(D_MINUS_1, False), (D, True)})


if __name__ == "__main__":
    unittest.main()
