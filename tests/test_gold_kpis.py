"""
test_gold_kpis.py – Gold KPI views and mart, checked against an independent calculation.

The SQL under test is the deployed SQL: liquibase/changelog/gold/*.sql. Locally the files are
run against Silver temp views built by the real Silver transforms from generated data; the only
rewrites are mechanical (drop the silver./gold. schema prefix, create session-scoped views,
MATERIALIZED VIEW -> view, since local Spark has no Unity Catalog).

Expected values are computed in plain Python straight from the generator records (clean records
only, identified via the injection manifests), not from Silver, so the SQL arithmetic is checked
against a separate implementation of each KPI definition.

Run locally with:
    python -m unittest tests.test_gold_kpis -v
"""

import json
import re
import unittest
import xml.dom.minidom
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from data_generators.generate_inventory import generate_inventory_rows, snapshot_nights
from data_generators.generate_master_data import build_master_data
from data_generators.generate_orders import expected_daily_units_per_sku, generate_order_lines, usable_master_keys
from data_generators.generate_shipments import build_network, generate_shipment_records, shippable_orders
from data_generators.produce_gps import build_fleet, generate_messages

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from local.preview.sql_local import localize_sql
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    from scm_platform.stream_gps_events import dedup_events, split_events
    from tests.test_stream_gps_events import kafka_frame
    from scm_platform.transform_silver import (
        generate_dim_date,
        transform_dim_carrier,
        transform_dim_customer,
        transform_dim_product,
        transform_dim_route,
        transform_dim_warehouse,
        transform_fact_inventory_snapshot,
        transform_fact_order,
        transform_fact_shipment,
        transform_gps_events,
        latest_ping_per_shipment,
    )

GOLD_DIR = Path(__file__).resolve().parent.parent / "liquibase" / "changelog" / "gold"
MASTER_CHANGELOG = GOLD_DIR.parent / "db.changelog-master.xml"
COUNTS = {"warehouses": 50, "carriers": 20, "products": 400, "customers": 300, "routes": 250}
ORDERS = 1500
NIGHTS = snapshot_nights(date(2026, 10, 12), 3)
SKUS = 40
INGEST_TS = datetime(2026, 10, 13, 2, 0)
TS = "%Y-%m-%dT%H:%M:%SZ"

PCT_TOLERANCE = 0.006   # Spark rounds HALF_UP, Python rounds half-to-even


def localize(sql: str) -> str:
    """Make deployed Unity Catalog SQL runnable on local Spark (mechanical rewrites only)."""
    return localize_sql(sql)


def _clean_after_injection(clean_records, key_fn, manifest, manifest_key_fn):
    """Records from a zero-corruption run, minus every key the manifest says was damaged.

    Generators draw clean values and corruption from separate RNGs, so a zero-corruption run
    reproduces every untouched record exactly; duplicates are not damage (one copy lands).
    """
    damaged = {manifest_key_fn(m) for m in manifest if m["failure_mode"] != "duplicate"}
    return {key_fn(r): r for r in clean_records if key_fn(r) not in damaged}


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required for Gold KPI tests")
class TestGoldKpis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("UnitTestGoldKpis")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")   # dates are UTC, as on Databricks
        spark = cls.spark

        # ------------------------------------------------------------- generated sources
        feeds, master_manifest = build_master_data(COUNTS, seed=42, bad_rate=0.02)
        keys = usable_master_keys(feeds, master_manifest)
        order_manifest, inventory_manifest, shipment_manifest = [], [], []
        lines = list(generate_order_lines(keys, ORDERS, seed=42, bad_rate=0.02, manifest=order_manifest,
                                          assortment_size=SKUS))
        # Stock is sized from the demand the orders feed implies, as generate_inventory.run does
        cls.mean_daily_units = expected_daily_units_per_sku(ORDERS, len(keys["warehouses"]), SKUS)
        inventory = [r for _, r in generate_inventory_rows(keys, NIGHTS, SKUS, seed=42, bad_rate=0.02,
                                                            manifest=inventory_manifest,
                                                            mean_daily_units=cls.mean_daily_units)]
        network = build_network(feeds["carriers"], feeds["warehouses"], feeds["routes"], master_manifest)
        damaged_orders = {m["order_id"] for m in order_manifest}
        shipments = list(generate_shipment_records(shippable_orders(iter(lines), damaged_orders), network,
                                                   seed=42, bad_rate=0.02, manifest=shipment_manifest))

        # GPS/RFID events for the in-transit trucks, stamped with the real current time because the
        # delay-risk GPS term compares against current_timestamp(). One truck never pings.
        damaged_shipments = {m["shipment_id"] for m in shipment_manifest if m["failure_mode"] != "duplicate"}
        fleet = build_fleet(shipments, feeds["routes"], damaged_shipments, seed=42)
        cls.unpinged_shipment = fleet[0]["shipment_id"]
        cls.gps_now = datetime.now(timezone.utc)
        cls.gps_messages = list(generate_messages(fleet[1:], 3000, seed=42, clock=lambda: cls.gps_now))

        # ------------------------------------------------------------- Silver (real transforms)
        def bronze(feed, records):
            return spark.createDataFrame(records, schema=DATASET_SCHEMAS[feed]()).withColumn(
                "_ingest_timestamp", lit(INGEST_TS))

        dims = {
            "dim_warehouse": transform_dim_warehouse(bronze("warehouses", feeds["warehouses"]))[0],
            "dim_product": transform_dim_product(bronze("products", feeds["products"]))[0],
            "dim_customer": transform_dim_customer(bronze("customers", feeds["customers"]))[0],
            "dim_carrier": transform_dim_carrier(bronze("carriers", feeds["carriers"]))[0],
            "dim_route": transform_dim_route(bronze("routes", feeds["routes"]))[0],
            "dim_date": generate_dim_date(spark),
        }
        dims = {name: df.cache() for name, df in dims.items()}
        facts = {
            "fact_order": transform_fact_order(
                bronze("orders", lines), dims["dim_customer"], dims["dim_product"], dims["dim_warehouse"])[0],
            "fact_inventory_snapshot": transform_fact_inventory_snapshot(
                bronze("inventory", inventory), dims["dim_warehouse"], dims["dim_product"])[0],
        }
        gps_events, _ = split_events(kafka_frame(spark, cls.gps_messages))
        gps_clean, _ = transform_gps_events(dedup_events(gps_events))
        facts["fact_shipment"] = transform_fact_shipment(
            bronze("shipments", shipments), dims["dim_carrier"], dims["dim_route"],
            df_dim_warehouse=dims["dim_warehouse"], df_last_ping=latest_ping_per_shipment(gps_clean))[0]
        for name, df in {**dims, **facts}.items():
            df.cache().createOrReplaceTempView(name)

        # ------------------------------------------------------------- Gold (deployed SQL)
        cls.gold_files = sorted(GOLD_DIR.glob("*.sql"))
        for path in cls.gold_files:
            spark.sql(localize(path.read_text(encoding="utf-8")))
        cls.views = {name: spark.table(name).collect() for name in (
            "vw_otif_daily", "vw_fill_rate_daily", "vw_stockout_rate_daily",
            "vw_inventory_turnover_daily", "vw_delivery_delay_risk", "mart_logistics_kpi_daily")}

        # ------------------------------------------------------------- independent clean records
        clean_lines = list(generate_order_lines(keys, ORDERS, seed=42, bad_rate=0.0, assortment_size=SKUS))
        cls.lines = _clean_after_injection(
            clean_lines, lambda l: (l["order_id"], l["order_line_number"]),
            order_manifest, lambda m: (m["order_id"], m["order_line_number"]))
        cls.inventory = _clean_after_injection(
            [r for _, r in generate_inventory_rows(keys, NIGHTS, SKUS, seed=42, bad_rate=0.0,
                                                   mean_daily_units=cls.mean_daily_units)],
            lambda r: (r["warehouse_id"], r["product_id"], r["snapshot_date"]),
            inventory_manifest, lambda m: (m["warehouse_id"], m["product_id"], m["snapshot_date"]))
        clean_shipments = generate_shipment_records(
            shippable_orders(iter(lines), damaged_orders), network, seed=42, bad_rate=0.0)
        cls.shipments = _clean_after_injection(
            clean_shipments, lambda s: s["shipment_id"], shipment_manifest, lambda m: m["shipment_id"])
        cls.unit_cost = {p["product_id"]: p["unit_cost"] for p in feeds["products"] if p.get("product_id")}
        cls.route_m = {r["route_id"]: r["distance_meters"] for r in feeds["routes"] if r.get("route_id")}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ helpers

    def assertPct(self, actual, expected, msg=None):
        self.assertIsNotNone(actual, msg)
        self.assertLessEqual(abs(float(actual) - expected), PCT_TOLERANCE, msg)

    def by_key(self, view):
        return {(r.warehouse_id, r.kpi_date.isoformat()): r for r in self.views[view]}

    # ------------------------------------------------------------------ structure

    def test_gold_never_reads_bronze(self):
        for path in self.gold_files:
            sql = path.read_text(encoding="utf-8")
            self.assertNotRegex(sql, r"\bbronze\.", f"{path.name} reads Bronze")
            for ref in re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", sql, flags=re.IGNORECASE):
                self.assertTrue(ref.startswith(("silver.", "gold.")) or "." not in ref,
                                f"{path.name} references {ref}")

    def test_every_gold_file_is_deployed_by_liquibase(self):
        doc = xml.dom.minidom.parse(str(MASTER_CHANGELOG))
        deployed = {n.getAttribute("path") for n in doc.getElementsByTagName("sqlFile")}
        self.assertEqual(deployed, {f"gold/{p.name}" for p in self.gold_files})
        for cs in doc.getElementsByTagName("changeSet"):
            self.assertEqual(cs.getAttribute("runOnChange"), "true")

    # ------------------------------------------------------------------ KPI 1: OTIF

    def test_otif_matches_independent_calculation(self):
        expected = defaultdict(lambda: [0, 0, 0, 0])
        for s in self.shipments.values():
            if s["shipment_status"] != "DELIVERED":
                continue
            on_time = s["actual_delivery_time_utc"] <= s["planned_delivery_time_utc"]
            in_full = s["shipped_quantity"] >= s["ordered_quantity"]
            e = expected[(s["warehouse_id"], s["actual_delivery_time_utc"][:10])]
            e[0] += 1; e[1] += on_time; e[2] += in_full; e[3] += on_time and in_full
        got = self.by_key("vw_otif_daily")
        self.assertEqual(set(got), set(expected))
        for key, (n, on_time, in_full, otif) in expected.items():
            r = got[key]
            self.assertEqual((r.shipments_delivered, r.on_time_shipments, r.in_full_shipments, r.otif_shipments),
                             (n, on_time, in_full, otif), key)
            self.assertPct(r.otif_pct, 100 * otif / n, key)
            self.assertPct(r.on_time_pct, 100 * on_time / n, key)

    # ------------------------------------------------------------------ KPI 2: fill rate

    def test_fill_rate_matches_independent_calculation(self):
        expected = defaultdict(lambda: [0, 0, 0, 0])
        for l in self.lines.values():
            if l["order_status"] not in ("SHIPPED", "DELIVERED"):
                continue
            e = expected[(l["warehouse_id"], l["order_date"])]
            e[0] += 1; e[1] += l["ordered_quantity"]; e[2] += l["shipped_quantity"]
            e[3] += l["shipped_quantity"] >= l["ordered_quantity"]
        got = self.by_key("vw_fill_rate_daily")
        self.assertEqual(set(got), set(expected))
        for key, (n, ordered, shipped, complete) in expected.items():
            r = got[key]
            self.assertEqual((r.lines_shipped, r.units_ordered, r.units_shipped), (n, ordered, shipped), key)
            self.assertPct(r.fill_rate_pct, 100 * shipped / ordered, key)
            self.assertPct(r.line_fill_rate_pct, 100 * complete / n, key)

    # ------------------------------------------------------------------ KPI 3: stockout rate

    def test_stockout_rate_matches_independent_calculation(self):
        expected = defaultdict(lambda: [0, 0, 0])
        for r in self.inventory.values():
            available = r["quantity_on_hand"] - r["quantity_reserved"]
            e = expected[(r["warehouse_id"], r["snapshot_date"])]
            e[0] += 1; e[1] += available <= 0; e[2] += available <= r["safety_stock"]
        got = self.by_key("vw_stockout_rate_daily")
        self.assertEqual(set(got), set(expected))
        for key, (n, out, risk) in expected.items():
            r = got[key]
            self.assertEqual((r.skus_stocked, r.stockout_skus, r.at_risk_skus), (n, out, risk), key)
            self.assertPct(r.stockout_rate_pct, 100 * out / n, key)
            self.assertPct(r.at_risk_pct, 100 * risk / n, key)
        self.assertGreater(sum(e[1] for e in expected.values()), 0, "data should contain stockouts")

    # ------------------------------------------------------------------ KPI 4: inventory turnover

    def test_inventory_turnover_matches_independent_calculation(self):
        dispatch_date = {s["order_id"]: s["dispatch_time_utc"][:10] for s in self.shipments.values()}
        cogs = defaultdict(float)
        for l in self.lines.values():
            if l["order_status"] in ("SHIPPED", "DELIVERED") and l["order_id"] in dispatch_date:
                cogs[(l["warehouse_id"], dispatch_date[l["order_id"]])] += \
                    l["shipped_quantity"] * self.unit_cost[l["product_id"]]
        value = defaultdict(float)
        for r in self.inventory.values():
            value[(r["warehouse_id"], r["snapshot_date"])] += r["quantity_on_hand"] * self.unit_cost[r["product_id"]]

        got = self.by_key("vw_inventory_turnover_daily")
        self.assertEqual(set(got), set(value))
        checked = 0
        for (wh, day), _ in value.items():
            end = date.fromisoformat(day)
            window = {(end - timedelta(days=i)).isoformat() for i in range(30)}
            cogs_30 = sum(v for (w, d), v in cogs.items() if w == wh and d in window)
            inv = [v for (w, d), v in value.items() if w == wh and d in window]
            avg_inv = sum(inv) / len(inv)
            r = got[(wh, day)]
            self.assertAlmostEqual(float(r.cogs_30d), cogs_30, delta=0.01)
            self.assertAlmostEqual(float(r.avg_inventory_value_30d), avg_inv, delta=0.01)
            self.assertAlmostEqual(float(r.inventory_turnover), cogs_30 / avg_inv * 365 / 30, delta=0.006)
            checked += cogs_30 > 0
        self.assertGreater(checked, 0, "some warehouse/day must have COGS in its window")

    def test_products_sold_are_products_stocked(self):
        """COGS and inventory value describe the same assortment, so turnover is meaningful."""
        stocked = defaultdict(set)
        for r in self.inventory.values():
            stocked[r["warehouse_id"]].add(r["product_id"])
        for l in self.lines.values():
            self.assertIn(l["product_id"], stocked[l["warehouse_id"]], (l["order_id"], l["warehouse_id"]))

    # ------------------------------------------------------------------ KPI 5: delay risk

    def test_delay_risk_matches_independent_calculation(self):
        delivered = [s for s in self.shipments.values() if s["shipment_status"] == "DELIVERED"]
        latest = max(datetime.strptime(s["actual_delivery_time_utc"], TS) for s in delivered)
        since = latest - timedelta(days=90)
        history = defaultdict(list)
        for s in delivered:
            if datetime.strptime(s["actual_delivery_time_utc"], TS) >= since:
                history[s["carrier_id"]].append(s["actual_delivery_time_utc"] <= s["planned_delivery_time_utc"])

        last_ping = {}
        for m in self.gps_messages:
            if m.startswith("{not-json"):
                continue
            e = json.loads(m)
            if e.get("truck_id") is None or e["lat"] == 999.0:
                continue
            last_ping[e["shipment_id"]] = max(last_ping.get(e["shipment_id"], ""), e["ts"])
        stale_before = (self.gps_now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")

        expected, expected_gps = {}, {}
        for s in self.shipments.values():
            if s["shipment_status"] != "IN_TRANSIT":
                continue
            ping = last_ping.get(s["shipment_id"])
            expected_gps[s["shipment_id"]] = ping is not None
            gps_stale = 1.0 if ping is None or ping[:19] < stale_before else 0.0
            weather = {"HIGH": 1.0, "MODERATE": 0.5}.get(s["weather_risk"], 0.1)
            hist = history.get(s["carrier_id"])
            on_time_rate = sum(hist) / len(hist) if hist else 0.8
            meters = self.route_m[s["route_id"]]
            distance = 1.0 if meters > 800000 else 0.5 if meters > 300000 else 0.15
            expected[s["shipment_id"]] = (0.35 * weather + 0.35 * (1 - on_time_rate) + 0.20 * distance
                                          + 0.10 * gps_stale)

        got = {r.shipment_id: r for r in self.views["vw_delivery_delay_risk"]}
        self.assertEqual(set(got), set(expected), "exactly the in-flight shipments are scored")
        for sid, score in expected.items():
            r = got[sid]
            self.assertAlmostEqual(float(r.delay_risk_score), score, delta=0.0011, msg=sid)
            band = "HIGH" if r.delay_risk_score >= 0.45 else "MEDIUM" if r.delay_risk_score >= 0.25 else "LOW"
            self.assertEqual(r.risk_band, band)
            self.assertEqual(r.gps_signal_available, expected_gps[sid], sid)
        self.assertFalse(got[self.unpinged_shipment].gps_signal_available)
        self.assertIsNone(got[self.unpinged_shipment].last_ping_ts)
        stale = [r for r in got.values() if r.gps_signal_available and r.minutes_since_last_ping >= 120]
        fresh = [r for r in got.values() if r.gps_signal_available and r.minutes_since_last_ping < 120]
        self.assertTrue(stale and fresh, "data should contain both dark (stale) and live trucks")

    # ------------------------------------------------------------------ mart

    def test_mart_has_one_row_per_warehouse_day_across_all_kpis(self):
        spine = set()
        for view in ("vw_otif_daily", "vw_fill_rate_daily", "vw_stockout_rate_daily", "vw_inventory_turnover_daily"):
            spine |= set(self.by_key(view))
        mart = self.by_key("mart_logistics_kpi_daily")
        self.assertEqual(len(mart), len(self.views["mart_logistics_kpi_daily"]), "duplicate warehouse/day rows")
        self.assertEqual(set(mart), spine)

    def test_mart_values_match_the_kpi_views(self):
        mart = self.by_key("mart_logistics_kpi_daily")
        for view, columns in [("vw_otif_daily", ["otif_pct", "on_time_pct", "in_full_pct", "shipments_delivered"]),
                              ("vw_fill_rate_daily", ["fill_rate_pct", "lines_shipped"]),
                              ("vw_stockout_rate_daily", ["stockout_rate_pct", "at_risk_pct", "skus_stocked"]),
                              ("vw_inventory_turnover_daily", ["inventory_turnover", "days_of_inventory"])]:
            source = self.by_key(view)
            for key, row in mart.items():
                for c in columns:
                    self.assertEqual(getattr(row, c), getattr(source[key], c) if key in source else None, (view, key, c))
        for row in mart.values():
            self.assertIsNotNone(row.warehouse_name)
            self.assertIsNotNone(row.country)

    def test_overall_kpis_are_in_realistic_ranges(self):
        otif = self.views["vw_otif_daily"]
        overall_otif = 100 * sum(r.otif_shipments for r in otif) / sum(r.shipments_delivered for r in otif)
        fill = self.views["vw_fill_rate_daily"]
        overall_fill = 100 * sum(r.units_shipped for r in fill) / sum(r.units_ordered for r in fill)
        stock = self.views["vw_stockout_rate_daily"]
        overall_stockout = 100 * sum(r.stockout_skus for r in stock) / sum(r.skus_stocked for r in stock)
        self.assertTrue(65 < overall_otif < 95, overall_otif)
        self.assertTrue(90 < overall_fill <= 100, overall_fill)
        self.assertTrue(0 < overall_stockout < 15, overall_stockout)
        # Stock and sales describe the same business: a DC turns stock roughly 4-20x a year
        turns = sorted(float(r.inventory_turnover) for r in self.views["vw_inventory_turnover_daily"]
                       if r.inventory_turnover is not None)
        median_turns = turns[len(turns) // 2]
        self.assertTrue(4 < median_turns < 20, median_turns)


if __name__ == "__main__":
    unittest.main()
