"""
test_reprocess_quarantine.py – Quarantine reprocessing: PENDING -> REPROCESSED / REJECTED.

Builds real quarantine rows from generated data with the real transforms, then replays them:
- a late-arriving dimension heals its facts,
- a still-unknown code stays PENDING (retry_count + 1),
- a steward correction (corrected_payload) heals and leaves original_payload untouched,
- the 7-day SLA rejects, duplicates and superseded copies are closed,
- a second run changes nothing it already resolved (idempotent),
- feeds that are not replayed (GPS) only age out.

Run locally with:
    python -m unittest tests.test_reprocess_quarantine -v
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import generate_order_lines, usable_master_keys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    from scm_platform.reprocess_quarantine import REJECTED, REPROCESSED, PENDING, reprocess, with_feed
    from scm_platform import transform_silver as ts

COUNTS = {"warehouses": 20, "carriers": 10, "products": 200, "customers": 150, "routes": 100}
ORDERS = 800
NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestReprocessQuarantine(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestReprocess")
                             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        feeds, manifest = build_master_data(COUNTS, seed=42, bad_rate=0.02)
        keys = usable_master_keys(feeds, manifest)
        lines = list(generate_order_lines(keys, ORDERS, seed=42, bad_rate=0.03, assortment_size=50))
        lit_ts = F.lit(datetime(2026, 10, 15, 2, 0))

        def bronze(feed, records):
            return spark.createDataFrame(records, schema=DATASET_SCHEMAS[feed]()).withColumn("_ingest_timestamp", lit_ts)

        cls.full_dims = {
            "dim_warehouse": ts.transform_dim_warehouse(bronze("warehouses", feeds["warehouses"]))[0].cache(),
            "dim_product": ts.transform_dim_product(bronze("products", feeds["products"]))[0].cache(),
            "dim_customer": ts.transform_dim_customer(bronze("customers", feeds["customers"]))[0].cache(),
        }
        # "Late" warehouse: missing from the dimension when the orders first arrive
        counts = {}
        for l in lines:
            if l["order_id"] and l["warehouse_id"] and l["warehouse_id"].startswith("WH-0"):
                counts[l["warehouse_id"]] = counts.get(l["warehouse_id"], 0) + 1
        cls.late_warehouse = max(sorted(counts), key=counts.get)
        early_dims = dict(cls.full_dims, dim_warehouse=cls.full_dims["dim_warehouse"]
                          .filter(F.col("warehouse_id") != cls.late_warehouse))

        _, quarantine = ts.transform_fact_order(bronze("orders", lines), early_dims["dim_customer"],
                                                early_dims["dim_product"], early_dims["dim_warehouse"])
        q = with_feed(quarantine, "orders").cache()

        # Steward corrects one zero-quantity line; one unit-price failure is already 8 days old
        qty_row = q.filter("failed_rule = 'RULE_ORDER_QUANTITY_POSITIVE'").orderBy("quarantine_id").first()
        fixed = json.loads(qty_row.original_payload)
        fixed["ordered_quantity"] = 5
        cls.corrected_id, cls.corrected_original = qty_row.quarantine_id, qty_row.original_payload
        cls.expired_id = q.filter("failed_rule = 'RULE_UNIT_PRICE_NON_NEGATIVE'").orderBy("quarantine_id").first().quarantine_id
        q = (q.withColumn("corrected_payload", F.when(F.col("quarantine_id") == cls.corrected_id, F.lit(json.dumps(fixed)))
                          .otherwise(F.col("corrected_payload")))
               .withColumn("resolved_by", F.when(F.col("quarantine_id") == cls.corrected_id, F.lit("steward@scm"))
                           .otherwise(F.col("resolved_by")))
               .withColumn("ingested_at", F.when(F.col("quarantine_id") == cls.expired_id, F.lit(NOW.replace(tzinfo=None) - timedelta(days=8)))
                           .otherwise(F.lit(NOW.replace(tzinfo=None) - timedelta(hours=3)))))

        # An older pending copy of one late-warehouse line (same key) and a GPS row (not replayed)
        late_row = q.filter("failed_rule = 'RULE_REF_INTEGRITY_WAREHOUSE'") \
            .filter(F.get_json_object("original_payload", "$.warehouse_id") == cls.late_warehouse).orderBy("quarantine_id").first()
        cls.superseded_id = "older-copy-" + late_row.quarantine_id
        older = q.filter(F.col("quarantine_id") == late_row.quarantine_id) \
            .withColumn("quarantine_id", F.lit(cls.superseded_id)) \
            .withColumn("ingested_at", F.lit(NOW.replace(tzinfo=None) - timedelta(days=1)))
        gps = q.limit(1).withColumn("quarantine_id", F.lit("gps-row")).withColumn("source_feed", F.lit("gps_events")) \
            .withColumn("failed_rule", F.lit("RULE_GPS_COORDINATES"))
        cls.before = q.unionByName(older).unionByName(gps).cache()

        cls.result = reprocess(cls.before, cls.full_dims, now=NOW)
        cls.after = {r.quarantine_id: r for r in cls.result.quarantine.collect()}
        cls.before_rows = {r.quarantine_id: r for r in cls.before.collect()}
        cls.healed_orders = cls.result.healed["fact_order"].collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def rows_with(self, rule, warehouse=None):
        out = []
        for qid, r in self.before_rows.items():
            if r.failed_rule != rule or qid in ("gps-row", self.superseded_id):
                continue
            if warehouse and json.loads(r.original_payload).get("warehouse_id") != warehouse:
                continue
            out.append(qid)
        return out

    def test_late_dimension_heals_its_facts(self):
        late = self.rows_with("RULE_REF_INTEGRITY_WAREHOUSE", self.late_warehouse)
        self.assertTrue(late)
        for qid in late:
            self.assertEqual(self.after[qid].status, REPROCESSED, qid)
            self.assertEqual(self.after[qid].resolution_note, "Auto-healed: passed the quality gate on retry")
            self.assertEqual(self.after[qid].resolved_by, "reprocess_quarantine")
        healed_keys = {(r.order_id, r.order_line_number) for r in self.healed_orders}
        for qid in late:
            p = json.loads(self.before_rows[qid].original_payload)
            self.assertIn((p["order_id"], p["order_line_number"]), healed_keys)

    def test_unknown_code_stays_pending_with_retry_count(self):
        unknown = self.rows_with("RULE_REF_INTEGRITY_WAREHOUSE", "WH-9999")
        self.assertTrue(unknown)
        for qid in unknown:
            self.assertEqual(self.after[qid].status, PENDING)
            self.assertEqual(self.after[qid].retry_count, 1)
            self.assertIsNotNone(self.after[qid].last_retry_at)

    def test_steward_correction_heals_and_original_is_untouched(self):
        r = self.after[self.corrected_id]
        self.assertEqual(r.status, REPROCESSED)
        self.assertEqual(r.original_payload, self.corrected_original)
        self.assertEqual(r.resolved_by, "steward@scm")
        p = json.loads(self.corrected_original)
        healed = [h for h in self.healed_orders if (h.order_id, h.order_line_number) == (p["order_id"], p["order_line_number"])]
        self.assertEqual(len(healed), 1)
        self.assertEqual(healed[0].ordered_quantity, 5)

    def test_sla_expiry_rejects(self):
        r = self.after[self.expired_id]
        self.assertEqual(r.status, REJECTED)
        self.assertIn("7-day quarantine SLA", r.resolution_note)
        self.assertIsNotNone(r.resolved_at)

    def test_duplicates_and_superseded_copies_are_closed(self):
        for qid in self.rows_with("RULE_DUPLICATE_KEY"):
            self.assertEqual(self.after[qid].status, REJECTED)
            self.assertIn("Duplicate", self.after[qid].resolution_note)
        self.assertEqual(self.after[self.superseded_id].status, REJECTED)
        self.assertIn("Superseded", self.after[self.superseded_id].resolution_note)

    def test_feeds_not_replayed_only_age_out(self):
        r = self.after["gps-row"]
        self.assertEqual(r.status, PENDING)
        self.assertEqual(r.retry_count, 0)

    def test_still_failing_rows_keep_failing_for_their_reason(self):
        for qid in self.rows_with("RULE_ORDER_STATUS_INVALID") + self.rows_with("RULE_ORDER_DATE_IN_FUTURE"):
            self.assertEqual(self.after[qid].status, PENDING)
            self.assertEqual(self.after[qid].failed_rule, self.before_rows[qid].failed_rule)

    def test_every_row_is_accounted_for(self):
        self.assertEqual(set(self.after), set(self.before_rows))
        self.assertTrue(set(r.status for r in self.after.values()) <= {PENDING, REPROCESSED, REJECTED})

    def test_second_run_is_idempotent(self):
        # Like the scheduled job, the second run reads the quarantine back as stored (shallow plan)
        stored = self.spark.createDataFrame(self.result.quarantine.collect(), self.result.quarantine.schema)
        again = reprocess(stored, self.full_dims, now=NOW)
        second = {r.quarantine_id: r.status for r in again.quarantine.collect()}
        self.assertEqual(second, {qid: r.status for qid, r in self.after.items()})
        self.assertEqual(again.healed["fact_order"].count(), 0, "nothing left to heal")


if __name__ == "__main__":
    unittest.main()
