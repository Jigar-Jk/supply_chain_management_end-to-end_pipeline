"""
test_transform_silver.py – Comprehensive Unit Tests for Silver Galaxy Schema Redesign.

Verifies:
1. Conformed dimensions:
   - silver.dim_date (static calendar generation)
   - silver.dim_route (SCD Type 1 overwrite, stable surrogate keys)
   - silver.dim_carrier (extraction, SCD Type 1, empty key quarantine)
   - silver.dim_customer & silver.dim_product (SCD Type 2 contracts)
2. Three fact tables:
   - silver.fact_shipment (grain: 1 row per leg, carrier_sk, route_sk, date_sk, transit calculation)
   - silver.fact_order (grain: 1 row per order line, customer_sk, product_sk, total_price calculation)
   - silver.fact_inventory_snapshot (grain: 1 row per warehouse/product/date, stockout risk flag)
3. Quality gates & charter quarantine:
   - Quarantine schema compliance: original_payload, failure_reason, failed_rule, batch_id, source_system, ingested_at, status
   - Status lifecycle flow: PENDING -> REPROCESSED or REJECTED
4. Reliability & idempotency:
   - Deterministic deduplication with tie-breaking
   - Stale-record protection (preventing older records from closing newer versions)
   - Repeated processing without duplicate dimensions or facts
   - Referential integrity failures routing to quarantine

Run locally with:
    python -m unittest tests.test_transform_silver -v
"""

import unittest
from datetime import date, datetime

# Guard PySpark imports for local test execution
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.transform_silver import (
        apply_scd2_dimension,
        deduplicate_dataframe,
        generate_dim_date,
        lookup_dimension_sk,
        transform_dim_carrier,
        transform_dim_customer,
        transform_dim_product,
        transform_dim_route,
        transform_dim_warehouse,
        transform_fact_inventory_snapshot,
        transform_fact_order,
        transform_fact_shipment,
        transform_routes,
        transform_weather,
        update_quarantine_status,
    )


class TestSilverGalaxySchema(unittest.TestCase):
    """Local unit test suite for Silver Medallion Galaxy Schema transformations."""

    @classmethod
    def setUpClass(cls):
        if not PYSPARK_AVAILABLE:
            return
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("UnitTestSilverGalaxySchema")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        if PYSPARK_AVAILABLE and hasattr(cls, "spark"):
            cls.spark.stop()

    def setUp(self):
        if not PYSPARK_AVAILABLE:
            self.skipTest("PySpark is required for Silver transformation unit tests.")

    # -----------------------------------------------------------------------
    # 1. Conformed Dimensions Tests
    # -----------------------------------------------------------------------

    def test_dim_date_generation(self):
        """silver.dim_date is a static Type 0 calendar dimension with standard date attributes."""
        dim_date = generate_dim_date(self.spark, start_date_str="2026-10-01", end_date_str="2026-10-03")
        self.assertEqual(dim_date.count(), 3)

        rows = dim_date.collect()
        r1 = rows[0]
        self.assertEqual(r1.date_sk, 20261001)
        self.assertEqual(r1.year, 2026)
        self.assertEqual(r1.quarter, 4)
        self.assertEqual(r1.month, 10)
        self.assertEqual(r1.month_name, "October")
        self.assertEqual(r1.day_of_month, 1)
        self.assertFalse(r1.is_holiday)

    def test_dim_route_scd1_overwrite_keeps_stable_surrogate_key(self):
        """silver.dim_route is SCD Type 1 (guide 8.1): attribute changes overwrite in place."""
        schema = self._route_schema()

        # Batch 1: Initial route load
        t1 = datetime(2026, 10, 1, 10, 0)
        batch1 = [("r1", 8.68, 49.41, 8.69, 49.42, 1500.0, 300.0, "driving-hgv", "ors", t1)]
        dim_v1, _ = transform_dim_route(self.spark.createDataFrame(batch1, schema=schema), df_existing_dim=None, batch_id="b1")
        self.assertEqual(dim_v1.count(), 1)
        rec_v1 = dim_v1.collect()[0]
        self.assertTrue(rec_v1.is_current)
        self.assertIsNone(rec_v1.effective_to)
        sk_v1 = rec_v1.route_sk

        # Batch 2: distance corrected from 1500 to 1800 at t2
        t2 = datetime(2026, 10, 5, 12, 0)
        batch2 = [("r1", 8.68, 49.41, 8.69, 49.42, 1800.0, 350.0, "driving-hgv", "ors", t2)]
        dim_v2, _ = transform_dim_route(self.spark.createDataFrame(batch2, schema=schema), df_existing_dim=dim_v1, batch_id="b2")

        self.assertEqual(dim_v2.count(), 1, "Type 1 keeps exactly one row per route")
        rec_v2 = dim_v2.collect()[0]
        self.assertEqual(rec_v2.route_sk, sk_v1, "Surrogate key is stable across overwrites")
        self.assertEqual(rec_v2.distance_meters, 1800.0)
        self.assertEqual(rec_v2.effective_from, t1, "effective_from records first sight of the key")
        self.assertTrue(rec_v2.is_current)

    def _route_schema(self):
        return StructType([
            StructField("route_id", StringType(), True),
            StructField("origin_longitude", DoubleType(), True),
            StructField("origin_latitude", DoubleType(), True),
            StructField("destination_longitude", DoubleType(), True),
            StructField("destination_latitude", DoubleType(), True),
            StructField("distance_meters", DoubleType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("routing_profile", StringType(), True),
            StructField("source", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])

    def _customer_schema(self):
        return StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])

    def test_dim_customer_scd2_versioning(self):
        """SCD Type 2 dims close the old version and open a new one on attribute change."""
        schema = self._customer_schema()
        t1, t2 = datetime(2026, 10, 1), datetime(2026, 10, 5)
        v1, _ = transform_dim_customer(
            self.spark.createDataFrame([("C_01", "Acme", "SMB", "DE", "Berlin", t1)], schema=schema)
        )
        v2, _ = transform_dim_customer(
            self.spark.createDataFrame([("C_01", "Acme", "ENTERPRISE", "DE", "Berlin", t2)], schema=schema),
            df_existing_dim=v1,
        )
        self.assertEqual(v2.count(), 2)
        closed = v2.filter("is_current = false").collect()[0]
        active = v2.filter("is_current = true").collect()[0]
        self.assertEqual(closed.customer_segment, "SMB")
        self.assertEqual(closed.effective_to, t2)
        self.assertEqual(active.customer_segment, "ENTERPRISE")
        self.assertEqual(active.effective_from, t2)
        self.assertNotEqual(active.customer_sk, closed.customer_sk)

    def test_scd2_reingest_unchanged_attributes_with_newer_timestamp_creates_no_version(self):
        """Regression: change detection must compare values, not inc_/curr_ prefixed field names."""
        schema = self._customer_schema()
        v1, _ = transform_dim_customer(
            self.spark.createDataFrame([("C_01", "Acme", "SMB", "DE", "Berlin", datetime(2026, 10, 1))], schema=schema)
        )
        v2, _ = transform_dim_customer(
            self.spark.createDataFrame([("C_01", "Acme", "SMB", "DE", "Berlin", datetime(2026, 10, 9))], schema=schema),
            df_existing_dim=v1,
        )
        self.assertEqual(v2.count(), 1, "Identical attributes re-ingested later must not create a new version")
        self.assertEqual(v2.collect()[0].customer_sk, v1.collect()[0].customer_sk)

    def test_fact_shipment_resolves_against_real_dims_loaded_after_dispatch(self):
        """Regression: dims built by the real transforms have effective_from = load time, which is
        after historical dispatch times. Shipments must still resolve rather than all failing RI."""
        route_rows = [("r1", 8.68, 49.41, 8.69, 49.42, 1500.0, 300.0, "driving-hgv", "ors", datetime(2026, 10, 12, 9, 0))]
        dim_route, _ = transform_dim_route(self.spark.createDataFrame(route_rows, schema=self._route_schema()))

        shipment_schema = StructType([
            StructField("shipment_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("carrier_id", StringType(), True),
            StructField("dispatch_time_utc", StringType(), True),
            StructField("planned_delivery_time_utc", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        shipments = self.spark.createDataFrame([
            ("s1", "r1", "CARRIER_DHL", "2026-09-15T10:00:00Z", "2026-09-15T15:00:00Z", "synthetic", datetime(2026, 10, 12, 9, 0)),
        ], schema=shipment_schema)
        dim_carrier, _ = transform_dim_carrier(shipments)  # effective_from = current_timestamp()

        fact_df, quar_df = transform_fact_shipment(shipments, dim_carrier, dim_route)
        self.assertEqual(quar_df.count(), 0, [r.failed_rule for r in quar_df.collect()])
        self.assertEqual(fact_df.count(), 1)

    def test_fact_order_point_in_time_scd2_lookup(self):
        """Orders resolve to the dimension version valid on the order date."""
        dim_cust = self.spark.createDataFrame([
            ("sk_c1_v1", "CUST_001", datetime(2026, 1, 1), datetime(2026, 6, 1), False),
            ("sk_c1_v2", "CUST_001", datetime(2026, 6, 1), None, True),
        ], schema="customer_sk string, customer_id string, effective_from timestamp, effective_to timestamp, is_current boolean")
        dim_prod = self.spark.createDataFrame(
            [("sk_p1", "SKU_100", datetime(2026, 1, 1), None, True)],
            schema="product_sk string, product_id string, effective_from timestamp, effective_to timestamp, is_current boolean",
        )
        orders = self.spark.createDataFrame([
            ("ORD_1", 1, "CUST_001", "SKU_100", "2026-03-10", 1, 1.0, "CREATED"),
            ("ORD_2", 1, "CUST_001", "SKU_100", "2026-09-10", 1, 1.0, "CREATED"),
        ], schema="order_id string, order_line_number int, customer_id string, product_id string, order_date string, ordered_quantity int, unit_price double, order_status string")
        fact, quar = transform_fact_order(orders, dim_cust, dim_prod)
        self.assertEqual(quar.count(), 0)
        sks = {r.order_id: r.customer_sk for r in fact.collect()}
        self.assertEqual(sks, {"ORD_1": "sk_c1_v1", "ORD_2": "sk_c1_v2"})

    def test_dim_carrier_extraction_and_empty_id_quarantine(self):
        """silver.dim_carrier extracts unique carriers and routes empty carrier_id to quarantine."""
        schema = StructType([
            StructField("carrier_id", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        rows = [
            ("CARRIER_DHL", datetime(2026, 10, 1, 10, 0)),
            ("CARRIER_DHL", datetime(2026, 10, 1, 11, 0)),  # Duplicate
            ("CARRIER_FEDEX", datetime(2026, 10, 1, 10, 0)),
            (None, datetime(2026, 10, 1, 10, 0)),  # Invalid
        ]
        df = self.spark.createDataFrame(rows, schema=schema)
        dim_carrier, quar = transform_dim_carrier(df, batch_id="b_carr")

        self.assertEqual(dim_carrier.count(), 2)
        carriers = {r.carrier_id for r in dim_carrier.collect()}
        self.assertEqual(carriers, {"CARRIER_DHL", "CARRIER_FEDEX"})

        self.assertEqual(quar.count(), 1)
        q_rec = quar.collect()[0]
        self.assertEqual(q_rec.failed_rule, "RULE_CARRIER_ID_EMPTY")
        self.assertEqual(q_rec.source_system, "CARRIER_API")
        self.assertEqual(q_rec.status, "PENDING")

    # -----------------------------------------------------------------------
    # 2. Fact Tables Tests
    # -----------------------------------------------------------------------

    def test_fact_shipment_grain_surrogate_keys_and_transit_calculation(self):
        """silver.fact_shipment grain: 1 row per leg, referencing carrier_sk, route_sk, date_sk."""
        # 1. Setup dim_carrier
        carrier_schema = StructType([
            StructField("carrier_sk", StringType(), False),
            StructField("carrier_id", StringType(), False),
            StructField("effective_from", TimestampType(), False),
            StructField("effective_to", TimestampType(), True),
            StructField("is_current", BooleanType(), False),
        ])
        dim_carrier = self.spark.createDataFrame(
            [("sk_dhl", "CARRIER_DHL", datetime(2026, 1, 1), None, True)],
            schema=carrier_schema,
        )

        # 2. Setup dim_route
        route_schema = StructType([
            StructField("route_sk", StringType(), False),
            StructField("route_id", StringType(), False),
            StructField("effective_from", TimestampType(), False),
            StructField("effective_to", TimestampType(), True),
            StructField("is_current", BooleanType(), False),
        ])
        dim_route = self.spark.createDataFrame(
            [("sk_r1", "r1", datetime(2026, 1, 1), None, True)],
            schema=route_schema,
        )

        # 3. Bronze shipment input
        shipment_schema = StructType([
            StructField("shipment_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("carrier_id", StringType(), True),
            StructField("dispatch_time_utc", StringType(), True),
            StructField("planned_delivery_time_utc", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        rows = [
            ("s1", "r1", "CARRIER_DHL", "2026-10-12T10:00:00Z", "2026-10-12T15:00:00Z", "synthetic", datetime(2026, 10, 12, 10, 0)),
        ]
        df_shipments = self.spark.createDataFrame(rows, schema=shipment_schema)

        fact_df, quar_df = transform_fact_shipment(
            df_shipments, dim_carrier, dim_route, batch_id="b_fact_s"
        )

        self.assertEqual(fact_df.count(), 1)
        self.assertEqual(quar_df.count(), 0)

        fact_rec = fact_df.collect()[0]
        self.assertEqual(fact_rec.shipment_id, "s1")
        self.assertEqual(fact_rec.leg_sequence, 1)
        self.assertEqual(fact_rec.carrier_sk, "sk_dhl")
        self.assertEqual(fact_rec.route_sk, "sk_r1")
        self.assertEqual(fact_rec.dispatch_date_sk, 20261012)
        self.assertEqual(fact_rec.planned_delivery_date_sk, 20261012)
        self.assertAlmostEqual(fact_rec.planned_transit_hours, 5.0, places=2)
        self.assertEqual(fact_rec.shipment_status, "DISPATCHED")

    def test_fact_shipment_referential_integrity_and_chronology_quarantine(self):
        """Shipments with non-existent route, carrier or chronological violations are quarantined."""
        carrier_df = self.spark.createDataFrame(
            [("sk_dhl", "CARRIER_DHL", datetime(2026, 1, 1), None, True)],
            schema="carrier_sk string, carrier_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )
        route_df = self.spark.createDataFrame(
            [("sk_r1", "r1", datetime(2026, 1, 1), None, True)],
            schema="route_sk string, route_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )

        shipment_schema = StructType([
            StructField("shipment_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("carrier_id", StringType(), True),
            StructField("dispatch_time_utc", StringType(), True),
            StructField("planned_delivery_time_utc", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        rows = [
            # Missing route FK
            ("s_bad_route", "route_unknown", "CARRIER_DHL", "2026-10-12T10:00:00Z", "2026-10-12T14:00:00Z", "synthetic", datetime(2026, 10, 1)),
            # Missing carrier FK
            ("s_bad_carrier", "r1", "CARRIER_UNKNOWN", "2026-10-12T10:00:00Z", "2026-10-12T14:00:00Z", "synthetic", datetime(2026, 10, 1)),
            # Delivery before dispatch
            ("s_bad_chrono", "r1", "CARRIER_DHL", "2026-10-12T15:00:00Z", "2026-10-12T10:00:00Z", "synthetic", datetime(2026, 10, 1)),
        ]
        df = self.spark.createDataFrame(rows, schema=shipment_schema)
        fact_df, quar_df = transform_fact_shipment(df, carrier_df, route_df, batch_id="b_test")

        self.assertEqual(fact_df.count(), 0)
        self.assertEqual(quar_df.count(), 3)

        rules = {r.failed_rule for r in quar_df.collect()}
        self.assertIn("RULE_REF_INTEGRITY_ROUTE", rules)
        self.assertIn("RULE_REF_INTEGRITY_CARRIER", rules)
        self.assertIn("RULE_CHRONOLOGICAL_DISPATCH", rules)

        for r in quar_df.collect():
            self.assertEqual(r.source_system, "CARRIER_API")
            self.assertEqual(r.status, "PENDING")
            self.assertIsNotNone(r.original_payload)

    def test_fact_order_grain_and_referential_integrity(self):
        """silver.fact_order grain: 1 row per order line, linking to customer_sk and product_sk."""
        dim_cust = self.spark.createDataFrame(
            [("sk_c1", "CUST_001", datetime(2026, 1, 1), None, True)],
            schema="customer_sk string, customer_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )
        dim_prod = self.spark.createDataFrame(
            [("sk_p1", "SKU_100", datetime(2026, 1, 1), None, True)],
            schema="product_sk string, product_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )

        orders_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_line_number", IntegerType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("ordered_quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("order_status", StringType(), True),
        ])
        rows = [
            ("ORD_1", 1, "CUST_001", "SKU_100", "2026-10-12", 10, 25.5, "CREATED"),
            ("ORD_1", 2, "CUST_001", "SKU_UNKNOWN", "2026-10-12", 5, 10.0, "CREATED"),  # Invalid product
        ]
        df_orders = self.spark.createDataFrame(rows, schema=orders_schema)
        fact_orders, quar = transform_fact_order(df_orders, dim_cust, dim_prod, batch_id="b_ord")

        self.assertEqual(fact_orders.count(), 1)
        r = fact_orders.collect()[0]
        self.assertEqual(r.order_id, "ORD_1")
        self.assertEqual(r.order_line_number, 1)
        self.assertEqual(r.customer_sk, "sk_c1")
        self.assertEqual(r.product_sk, "sk_p1")
        self.assertEqual(r.total_price, 255.0)

        self.assertEqual(quar.count(), 1)
        self.assertEqual(quar.collect()[0].failed_rule, "RULE_REF_INTEGRITY_PRODUCT")

    def test_fact_inventory_snapshot_grain_and_stockout_risk_flag(self):
        """silver.fact_inventory_snapshot grain: 1 row per warehouse, product, snapshot date."""
        dim_wh = self.spark.createDataFrame(
            [("sk_wh1", "WH_CENTRAL", datetime(2026, 1, 1), None, True)],
            schema="warehouse_sk string, warehouse_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )
        dim_prod = self.spark.createDataFrame(
            [("sk_p1", "SKU_100", datetime(2026, 1, 1), None, True)],
            schema="product_sk string, product_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        )

        inv_schema = StructType([
            StructField("warehouse_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("snapshot_date", StringType(), True),
            StructField("quantity_on_hand", DoubleType(), True),
            StructField("quantity_reserved", DoubleType(), True),
            StructField("safety_stock", DoubleType(), True),
            StructField("reorder_point", DoubleType(), True),
        ])
        rows = [
            # Normal stock
            ("WH_CENTRAL", "SKU_100", "2026-10-12", 100.0, 20.0, 30.0, 50.0),
            # Stockout risk (avail 20 <= safety 30)
            ("WH_CENTRAL", "SKU_100", "2026-10-13", 40.0, 20.0, 30.0, 50.0),
        ]
        df = self.spark.createDataFrame(rows, schema=inv_schema)
        fact_inv, _ = transform_fact_inventory_snapshot(df, dim_wh, dim_prod, batch_id="b_inv")

        self.assertEqual(fact_inv.count(), 2)
        r1, r2 = sorted(fact_inv.collect(), key=lambda x: x.snapshot_date_sk)
        self.assertEqual(r1.quantity_available, 80.0)
        self.assertFalse(r1.is_stockout_risk)

        self.assertEqual(r2.quantity_available, 20.0)
        self.assertTrue(r2.is_stockout_risk)

    # -----------------------------------------------------------------------
    # 3. Reliability, Deduplication & Stale-Record Protection
    # -----------------------------------------------------------------------

    def test_stale_record_protection_in_scd2(self):
        """An incoming record with an older effective_from does not overwrite a newer active version."""
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])

        # Active current dimension record with timestamp Oct 10
        active_dim = self.spark.createDataFrame([
            ("C_01", "Acme New", "ENTERPRISE", "DE", "Berlin", datetime(2026, 10, 10, 0, 0))
        ], schema=schema)
        dim_current, _ = transform_dim_customer(active_dim, df_existing_dim=None)

        # Stale record arriving late with timestamp Oct 5 (older than Oct 10)
        stale_df = self.spark.createDataFrame([
            ("C_01", "Acme Old", "SMB", "DE", "Berlin", datetime(2026, 10, 5, 0, 0))
        ], schema=schema)

        dim_after_stale, _ = transform_dim_customer(stale_df, df_existing_dim=dim_current)
        # Active record must remain Acme New
        active = dim_after_stale.filter("is_current = true").collect()[0]
        self.assertEqual(active.customer_name, "Acme New")

    def test_repeated_processing_idempotency_no_duplicate_versions(self):
        """Reprocessing identical data creates zero duplicate dimension versions or fact rows."""
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("origin_longitude", DoubleType(), True),
            StructField("origin_latitude", DoubleType(), True),
            StructField("destination_longitude", DoubleType(), True),
            StructField("destination_latitude", DoubleType(), True),
            StructField("distance_meters", DoubleType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("routing_profile", StringType(), True),
            StructField("source", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        rows = [("r1", 8.68, 49.41, 8.69, 49.42, 1500.0, 300.0, "driving-hgv", "ors", datetime(2026, 10, 1, 10, 0))]
        df = self.spark.createDataFrame(rows, schema=schema)

        dim1, _ = transform_dim_route(df, df_existing_dim=None)
        self.assertEqual(dim1.count(), 1)

        # Reprocess identical batch against dim1
        dim2, _ = transform_dim_route(df, df_existing_dim=dim1)
        self.assertEqual(dim2.count(), 1, "Idempotent: no duplicate row created when attributes are unchanged")

    def test_quarantine_status_lifecycle_transition(self):
        """Quarantine records support status flow: PENDING -> REPROCESSED or REJECTED."""
        quar_schema = StructType([
            StructField("quarantine_id", StringType(), False),
            StructField("status", StringType(), False),
        ])
        df_q = self.spark.createDataFrame([
            ("q1", "PENDING"),
            ("q2", "PENDING"),
        ], schema=quar_schema)

        reprocessed = update_quarantine_status(df_q, ["q1"], "REPROCESSED")
        statuses = {r.quarantine_id: r.status for r in reprocessed.collect()}
        self.assertEqual(statuses["q1"], "REPROCESSED")
        self.assertEqual(statuses["q2"], "PENDING")

        rejected = update_quarantine_status(reprocessed, ["q2"], "REJECTED")
        statuses2 = {r.quarantine_id: r.status for r in rejected.collect()}
        self.assertEqual(statuses2["q2"], "REJECTED")

        # Invalid transition throws ValueError
        with self.assertRaises(ValueError):
            update_quarantine_status(df_q, ["q1"], "INVALID_STATUS")

    def test_weather_and_routes_legacy_adapter(self):
        """Preserve existing weather transformations and route legacy adapters."""
        w_schema = StructType([
            StructField("weather_record_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("forecast_time_utc", StringType(), True),
            StructField("temperature_2m_c", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("wind_speed_10m_kmh", DoubleType(), True),
            StructField("visibility_m", DoubleType(), True),
            StructField("source", StringType(), True),
            StructField("_ingest_timestamp", TimestampType(), True),
        ])
        rows = [("w1", 49.42, 8.69, "2026-10-12T00:00:00Z", 7.9, 0.0, 2.8, 25000.0, "open-meteo", datetime(2026, 10, 1, 10, 0))]
        df_w = self.spark.createDataFrame(rows, schema=w_schema)
        silver_w, quar_w = transform_weather(df_w)
        self.assertEqual(silver_w.count(), 1)
        self.assertEqual(quar_w.count(), 0)


if __name__ == "__main__":
    unittest.main()
