"""
test_cdc_silver.py – Applying Debezium CDC events to Silver (fact_order, inventory_current).

Events are built in the exact format of the real Debezium SQL Server messages captured in
tests/fixtures_debezium_order_lines.json, then run through the real Bronze parser
(split_cdc_events) and the Silver apply. Each test replays one situation on purpose:
out-of-order delivery, Debezium redelivery, insert-then-delete, bad values arriving via CDC.

Run locally with:
    python -m unittest tests.test_cdc_silver -v
"""

import json
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.stream_cdc_events import split_cdc_events
    from scm_platform.transform_silver import (
        decode_inventory_cdc,
        decode_order_line_cdc,
        merge_update_condition,
        transform_fact_order,
        transform_inventory_current,
    )
    from tests.test_stream_cdc_events import kafka_rows

FIXTURES = json.loads((Path(__file__).parent / "fixtures_debezium_order_lines.json").read_text(encoding="utf-8"))
BASE_LINE = json.loads(FIXTURES["u"]["value"])["after"]     # a real order line image
TS_MS = 1792069500000


def lsn(n: int) -> str:
    """Fixed-width LSN like Debezium's, increasing with n."""
    return f"00000040:{n:08x}:0001"


def event(op, before, after, n, table="order_lines", snapshot=False):
    """A Debezium SQL Server change event as JSON, in the captured real format."""
    return json.dumps({
        "before": before, "after": after, "op": op, "ts_ms": TS_MS + n, "transaction": None,
        "source": {"version": "3.0.0.Final", "connector": "sqlserver", "name": "scm", "ts_ms": TS_MS + n,
                   "snapshot": "true" if snapshot else "false", "db": "scm_oms", "schema": "dbo", "table": table,
                   "commit_lsn": lsn(n), "change_lsn": None if snapshot else lsn(n), "event_serial_no": None if snapshot else 1},
    })


def line(order_id, line_no=1, **changes):
    row = dict(BASE_LINE, order_id=order_id, order_line_number=line_no)
    row.update(changes)
    return row


def key(row):
    return json.dumps({"order_id": row["order_id"], "order_line_number": row["order_line_number"]})


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestCdcSilverApply(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestCdcSilver")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        dim = "{0}_sk string, {0}_id string, effective_from timestamp, effective_to timestamp, is_current boolean"
        early = datetime(2020, 1, 1)
        cls.dims = {
            "customer": cls.spark.createDataFrame([("sk_c", BASE_LINE["customer_id"], early, None, True)], dim.format("customer")),
            "product": cls.spark.createDataFrame([("sk_p", BASE_LINE["product_id"], early, None, True)], dim.format("product")),
            "warehouse": cls.spark.createDataFrame([("sk_w", BASE_LINE["warehouse_id"], early, None, True)], dim.format("warehouse")),
        }

        created = line("ORD-A", order_status="CREATED", shipped_quantity=0)
        picked = line("ORD-A", order_status="PICKED", shipped_quantity=0)
        shipped = line("ORD-A", order_status="SHIPPED", shipped_quantity=24)
        to_delete = line("ORD-B", order_status="CREATED", shipped_quantity=0)
        redelivered = line("ORD-C", order_status="DELIVERED")
        cls.messages = [
            # ORD-A: snapshot -> picked -> shipped, then a STALE replay of "picked" arrives last
            (key(created), event("r", None, created, 1, snapshot=True)),
            (key(picked), event("u", created, picked, 3)),
            (key(shipped), event("u", picked, shipped, 5)),
            (key(picked), event("u", created, picked, 3)),                       # out-of-order replay
            # ORD-B: inserted, then deleted (+ the tombstone Debezium sends after a delete)
            (key(to_delete), event("c", None, to_delete, 2)),
            (key(to_delete), event("d", to_delete, None, 6)),
            (key(to_delete), None),
            # ORD-C: Debezium re-sends the same event after a crash
            (key(redelivered), event("u", shipped, redelivered, 7)),
            (key(redelivered), event("u", shipped, redelivered, 7)),
            # Bad values arriving through CDC must hit the quality gate
            (key(line("ORD-D")), event("u", None, line("ORD-D", ordered_quantity=-5), 8)),
            (key(line("ORD-E")), event("u", None, line("ORD-E", order_status="SHIPPPED"), 9)),
        ]
        events, _ = split_cdc_events(kafka_rows(cls.spark, cls.messages))
        cls.decoded = decode_order_line_cdc(events).cache()
        fact, quarantine = transform_fact_order(cls.decoded, cls.dims["customer"], cls.dims["product"],
                                                cls.dims["warehouse"], batch_id="b_cdc")
        cls.fact = {r.order_id: r for r in fact.collect()}
        cls.quarantine = quarantine.collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ decoding

    def test_debezium_encodings_are_decoded(self):
        r = self.decoded.filter("order_id = 'ORD-A' AND _op = 'r'").collect()[0]
        expected_date = date(1970, 1, 1).toordinal() + BASE_LINE["order_date"]
        self.assertEqual(r.order_date, date.fromordinal(expected_date).isoformat())
        # PySpark returns naive local-time datetimes; astimezone() treats them as local and converts
        self.assertEqual(round(r.updated_at.astimezone(timezone.utc).timestamp() * 1000), BASE_LINE["updated_at"])

    def test_real_fixtures_decode_including_delete_from_before_image(self):
        real = [(FIXTURES[op]["key"], FIXTURES[op]["value"]) for op in ("r", "c", "u", "d", "tombstone")]
        events, _ = split_cdc_events(kafka_rows(self.spark, real))
        rows = {r._op: r for r in decode_order_line_cdc(events).collect()}
        self.assertEqual(set(rows), {"r", "c", "u", "d"})
        deleted = json.loads(FIXTURES["d"]["value"])["before"]
        self.assertEqual((rows["d"].order_id, rows["d"].order_line_number), (deleted["order_id"], deleted["order_line_number"]))
        self.assertTrue(rows["d"]._is_deleted)
        self.assertFalse(rows["u"]._is_deleted)

    # ------------------------------------------------------------------ apply semantics

    def test_out_of_order_replay_cannot_overwrite_newer_state(self):
        a = self.fact["ORD-A"]
        self.assertEqual(a.order_status, "SHIPPED")
        self.assertEqual(a.shipped_quantity, 24)
        self.assertEqual(a.source_position, f"{lsn(5)}|{lsn(5)}|000001")

    def test_delete_becomes_soft_delete_with_last_known_values(self):
        b = self.fact["ORD-B"]
        self.assertTrue(b.is_deleted)
        self.assertEqual(b.order_status, "CREATED")

    def test_redelivered_event_is_collapsed_not_quarantined(self):
        self.assertEqual(self.fact["ORD-C"].order_status, "DELIVERED")
        self.assertFalse(any(q.failed_rule == "RULE_DUPLICATE_KEY" for q in self.quarantine))

    def test_bad_values_via_cdc_hit_the_quality_gate(self):
        rules = {json.loads(q.original_payload)["order_id"]: q.failed_rule for q in self.quarantine}
        self.assertEqual(rules, {"ORD-D": "RULE_ORDER_QUANTITY_POSITIVE", "ORD-E": "RULE_ORDER_STATUS_INVALID"})
        self.assertNotIn("ORD-D", self.fact)
        self.assertNotIn("ORD-E", self.fact)

    def test_quarantine_payload_has_no_cdc_working_columns(self):
        payload = json.loads(self.quarantine[0].original_payload)
        for c in ("_op", "_is_deleted", "_source_position", "_grain_key"):
            self.assertNotIn(c, payload)

    def test_live_rows_are_not_deleted(self):
        for order_id in ("ORD-A", "ORD-C"):
            self.assertFalse(self.fact[order_id].is_deleted)

    def test_merge_condition_only_moves_forward(self):
        cond = merge_update_condition("source_position")
        rows = [(None, "b", True), ("b", "a", False), ("a", "b", True), ("a", "a", True)]
        df = self.spark.createDataFrame(rows, "t string, s string, expected boolean")
        df = df.selectExpr("t", "s", "expected", cond.replace("target.source_position", "t")
                           .replace("source.source_position", "s") + " AS allowed")
        for r in df.collect():
            self.assertEqual(bool(r.allowed), r.expected, (r.t, r.s))

    # ------------------------------------------------------------------ inventory_current

    def test_inventory_current_latest_position_soft_delete_and_gate(self):
        def stock(product, on_hand, reserved=0):
            return {"warehouse_id": "WH-0001", "product_id": product, "quantity_on_hand": on_hand,
                    "quantity_reserved": reserved, "safety_stock": 5, "reorder_point": 10, "updated_at": TS_MS}

        def k(row):
            return json.dumps({"warehouse_id": row["warehouse_id"], "product_id": row["product_id"]})

        msgs = [
            (k(stock("SKU-1", 50)), event("r", None, stock("SKU-1", 50), 1, table="inventory", snapshot=True)),
            (k(stock("SKU-1", 20)), event("u", stock("SKU-1", 50), stock("SKU-1", 20, 4), 4, table="inventory")),
            (k(stock("SKU-1", 50)), event("u", stock("SKU-1", 60), stock("SKU-1", 50), 2, table="inventory")),  # stale
            (k(stock("SKU-2", 10)), event("d", stock("SKU-2", 10), None, 5, table="inventory")),
            (k(stock("SKU-3", -3)), event("u", None, stock("SKU-3", -3), 6, table="inventory")),
        ]
        events, _ = split_cdc_events(kafka_rows(self.spark, msgs))
        current, quarantine = transform_inventory_current(decode_inventory_cdc(events))
        rows = {r.product_id: r for r in current.collect()}
        self.assertEqual((rows["SKU-1"].quantity_on_hand, rows["SKU-1"].quantity_available), (20, 16))
        self.assertTrue(rows["SKU-2"].is_deleted)
        self.assertNotIn("SKU-3", rows)
        self.assertEqual([q.failed_rule for q in quarantine.collect()], ["RULE_QTY_ON_HAND_NON_NEGATIVE"])


if __name__ == "__main__":
    unittest.main()
