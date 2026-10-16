"""
test_silver_quality_tests.py – The table-level Silver checks run by scm_daily_pipeline.

The SQL under test is the deployed file databricks/sql/silver_quality_tests.sql. Locally it runs
against Silver temp views built by the real transforms from generated data; the only rewrites are
mechanical (no USE CATALOG, schema prefixes via localize_sql, a fixed as_of time so the freshness
checks do not depend on the wall clock).

1. Clean Silver passes every check.
2. Each kind of corruption, introduced on purpose, fails the task and names the right check.

Run locally with:
    python -m unittest tests.test_silver_quality_tests -v
"""

import re
import unittest
from datetime import date
from pathlib import Path

from data_generators.generate_inventory import generate_inventory_rows, snapshot_nights
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import generate_order_lines, usable_master_keys
from data_generators.generate_shipments import build_network, generate_shipment_records, shippable_orders
from local.preview.sql_local import local_name, localize_sql

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform import transform_silver as ts
    from scm_platform.ingest_bronze import DATASET_SCHEMAS

SQL_FILE = Path(__file__).resolve().parent.parent / "databricks" / "sql" / "silver_quality_tests.sql"
COUNTS = {"warehouses": 12, "carriers": 5, "products": 120, "customers": 60, "routes": 40}
AS_OF = "2026-10-12 03:00:00"                      # daily run time; generated data ends 2026-10-11 23:48
INGEST_TS = "2026-10-12 02:00:00"
NIGHTS = snapshot_nights(date(2026, 10, 12), 2)


def statements(sql: str):
    """Deployed file -> local statements: drop USE CATALOG, pin as_of, localize schema names."""
    sql = re.sub(r"^USE CATALOG .*?;\s*$", "", sql, flags=re.MULTILINE)
    sql = sql.replace("DEFAULT current_timestamp()", f"DEFAULT timestamp'{AS_OF}'")
    sql = "\n".join(line for line in localize_sql(sql).splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", sql) if s.strip()]


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestSilverQualityTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestSilverQuality")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC")
                             # ~25 joins in one query: broadcasting them all at once exhausts a small local driver
                             .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate())
        utc = F.to_utc_timestamp(F.lit(INGEST_TS).cast("timestamp"), F.current_timezone())

        feeds, master_manifest = build_master_data(COUNTS, seed=42, bad_rate=0.02)
        keys = usable_master_keys(feeds, master_manifest)
        lines = list(generate_order_lines(keys, 400, seed=42, bad_rate=0.02, assortment_size=15))
        inventory = [r for _, r in generate_inventory_rows(keys, NIGHTS, 15, seed=42, bad_rate=0.02)]
        network = build_network(feeds["carriers"], feeds["warehouses"], feeds["routes"], master_manifest)
        shipments = list(generate_shipment_records(shippable_orders(iter(lines), set()), network,
                                                   seed=42, bad_rate=0.02))

        def bronze(feed, records):
            return spark.createDataFrame(records, schema=DATASET_SCHEMAS[feed]()).withColumn("_ingest_timestamp", utc)

        b = {feed: bronze(feed, recs).cache() for feed, recs in [
            ("warehouses", feeds["warehouses"]), ("products", feeds["products"]), ("customers", feeds["customers"]),
            ("carriers", feeds["carriers"]), ("routes", feeds["routes"]), ("orders", lines),
            ("inventory", inventory), ("shipments", shipments)]}

        quarantine = []
        def keep(feed, result):
            silver, quar = result[0], result[1]
            quarantine.append(ts.tag_quarantine_feed(quar, feed))
            return silver.cache()

        silver = {
            "dim_warehouse": keep("warehouses", ts.transform_dim_warehouse(b["warehouses"])),
            "dim_product": keep("products", ts.transform_dim_product(b["products"])),
            "dim_customer": keep("customers", ts.transform_dim_customer(b["customers"])),
            "dim_carrier": keep("carriers", ts.transform_dim_carrier(b["carriers"])),
            "dim_route": keep("routes", ts.transform_dim_route(b["routes"])),
            "dim_date": ts.generate_dim_date(spark).cache(),
        }
        silver["fact_order"] = keep("orders", ts.transform_fact_order(
            b["orders"], silver["dim_customer"], silver["dim_product"], silver["dim_warehouse"]))
        silver["fact_inventory_snapshot"] = keep("inventory", ts.transform_fact_inventory_snapshot(
            b["inventory"], silver["dim_warehouse"], silver["dim_product"]))
        silver["fact_shipment"] = keep("shipments", ts.transform_fact_shipment(
            b["shipments"], silver["dim_carrier"], silver["dim_route"], df_dim_warehouse=silver["dim_warehouse"]))
        # inventory_current comes from the CDC feed; here: the latest night per warehouse and product
        silver["inventory_current"] = silver["fact_inventory_snapshot"].filter(F.col("snapshot_date") == F.lit(NIGHTS[-1])) \
            .select(F.md5(F.concat_ws("_", "warehouse_id", "product_id")).alias("inventory_current_sk"),
                    "warehouse_id", "product_id", "quantity_on_hand", F.lit(False).alias("is_deleted")).cache()

        # Rebuild from collected rows: the ~50-check UNION over the full transform lineage of every
        # view is too deep for a small local driver (OutOfMemoryError while planning).
        def flat(df):
            return spark.createDataFrame(df.collect(), df.schema).cache()
        silver = {name: flat(df) for name, df in silver.items()}
        cls.silver = silver
        for name, df in silver.items():
            df.createOrReplaceTempView(local_name("silver", name))
        for feed in ("orders", "inventory", "shipments"):
            flat(b[feed]).createOrReplaceTempView(local_name("bronze", feed))
        spark.createDataFrame([], "_ingest_timestamp timestamp").createOrReplaceTempView(local_name("bronze", "orders_cdc"))
        q = quarantine[0]
        for more in quarantine[1:]:
            q = q.unionByName(more)
        cls.quarantine = flat(q.withColumn("ingested_at", utc + F.expr("INTERVAL 5 MINUTES")))
        cls.quarantine.createOrReplaceTempView(local_name("quarantine", "records"))
        cls.statements = statements(SQL_FILE.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ helpers

    def run_file(self):
        """Run every statement; return the results view as {check_name: failing_rows}."""
        for stmt in self.statements[:-1]:
            self.spark.sql(stmt).collect()
        results = {r.check_name: r.failing_rows for r in self.spark.table("silver_quality_results").collect()}
        self.spark.sql(self.statements[-1]).collect()        # raise_error when anything failed
        return results

    def assertCaught(self, check_name, view, df, schema="silver"):
        """Replace one table with a corrupted copy: the file must fail and name the check."""
        name = local_name(schema, view)
        original = self.silver[view] if schema == "silver" else self.quarantine
        df.createOrReplaceTempView(name)
        try:
            with self.assertRaises(Exception) as ctx:
                self.run_file()
            self.assertIn("Silver quality tests failed", str(ctx.exception))
            self.assertIn(check_name, str(ctx.exception))
        finally:
            original.createOrReplaceTempView(name)

    # ------------------------------------------------------------------ clean data

    def test_clean_silver_passes_every_check(self):
        results = self.run_file()
        self.assertEqual({k: v for k, v in results.items() if v}, {})
        self.assertGreaterEqual(len(results), 40)
        for category in ("uniqueness", "scd2", "referential", "business_rule", "quarantine_rate", "freshness"):
            self.assertTrue(self.spark.table("silver_quality_results").filter(F.col("category") == category).count(),
                            f"no {category} checks")

    def test_quarantine_rate_is_checked_for_each_fact_feed(self):
        self.run_file()
        names = [r.check_name for r in self.spark.table("silver_quality_results").collect()]
        for feed in ("orders", "inventory", "shipments"):
            self.assertTrue(any(n.startswith("quarantine rate") and f"({feed})" in n for n in names), feed)

    # ------------------------------------------------------------------ each corruption is caught

    def test_duplicate_order_line_is_caught(self):
        fo = self.silver["fact_order"]
        self.assertCaught("fact_order.order_line_sk unique", "fact_order", fo.unionByName(fo.limit(1)))

    def test_second_current_warehouse_version_is_caught(self):
        wh = self.silver["dim_warehouse"]
        extra = wh.limit(1).withColumn("warehouse_sk", F.lit("sk-extra")) \
            .withColumn("effective_from", F.col("effective_from") + F.expr("INTERVAL 1 DAY"))
        self.assertCaught("dim_warehouse exactly one current version", "dim_warehouse", wh.unionByName(extra))

    def test_overlapping_versions_are_caught(self):
        p = self.silver["dim_product"]
        first = p.orderBy("product_id").limit(1)
        old = first.withColumn("product_sk", F.lit("sk-old")).withColumn("is_current", F.lit(False)) \
            .withColumn("effective_from", F.col("effective_from") - F.expr("INTERVAL 10 DAYS")) \
            .withColumn("effective_to", F.col("effective_from") + F.expr("INTERVAL 20 DAYS"))   # ends after the next starts
        self.assertCaught("dim_product versions do not overlap", "dim_product", p.unionByName(old))

    def test_orphan_customer_key_is_caught(self):
        c = self.silver["dim_customer"]
        used = self.silver["fact_order"].select("customer_sk").first()[0]
        self.assertCaught("fact_order.customer_sk in dim_customer", "dim_customer", c.filter(F.col("customer_sk") != used))

    def test_over_shipped_order_is_caught(self):
        fo = self.silver["fact_order"]
        bad = fo.withColumn("shipped_quantity", F.when(F.monotonically_increasing_id() == 0, F.col("ordered_quantity") + 1)
                            .otherwise(F.col("shipped_quantity")))
        self.assertCaught("fact_order 0 <= shipped_quantity <= ordered_quantity", "fact_order", bad)

    def test_inconsistent_available_stock_is_caught(self):
        inv = self.silver["fact_inventory_snapshot"]
        self.assertCaught("available = on_hand - reserved", "fact_inventory_snapshot",
                          inv.withColumn("quantity_available", F.col("quantity_available") + 1))

    def test_quarantine_spike_is_caught(self):
        q = self.quarantine.filter(F.col("source_feed") == "orders")
        spike = q
        for _ in range(12):                                   # an upstream breakage: most of the feed quarantined
            spike = spike.unionByName(q)
        self.assertCaught("(orders)", "records", self.quarantine.unionByName(spike), schema="quarantine")

    def test_stalled_source_is_caught(self):
        fs = self.silver["fact_shipment"]
        stale = fs.withColumn("updated_at", F.col("updated_at") - F.expr("INTERVAL 3 DAYS"))
        self.assertCaught("fact_shipment newest carrier update within 26 h", "fact_shipment", stale)

    def test_empty_fact_is_caught(self):
        fo = self.silver["fact_order"]
        self.assertCaught("fact_order newest source change", "fact_order", fo.limit(0))

    def test_missing_wms_night_is_caught(self):
        inv = self.silver["fact_inventory_snapshot"]
        self.assertCaught("fact_inventory_snapshot has last night", "fact_inventory_snapshot",
                          inv.filter(F.col("snapshot_date") < F.lit(NIGHTS[-1])))    # only the night before

    # ------------------------------------------------------------------ deployability

    def test_file_is_catalog_independent(self):
        sql = SQL_FILE.read_text(encoding="utf-8")
        self.assertRegex(sql, r"USE CATALOG IDENTIFIER\(:catalog\);")
        self.assertNotRegex(sql, r"scm_platform_(dev|qa|prod)")
        for ref in re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)\.", sql):
            self.assertIn(ref, ("silver", "bronze", "quarantine"))


if __name__ == "__main__":
    unittest.main()
