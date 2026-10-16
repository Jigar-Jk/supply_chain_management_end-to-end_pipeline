"""
test_stream_cdc_events.py – CDC Bronze parsing on REAL Debezium messages, no broker needed.

tests/fixtures_debezium_order_lines.json holds one real message of each kind captured from the
local Debezium SQL Server connector: snapshot read (r), insert (c), update (u), delete (d) and the
tombstone that follows a delete. They are fed through split_cdc_events as a batch DataFrame with
the Kafka source schema.

Run locally with:
    python -m unittest tests.test_stream_cdc_events -v
"""

import json
import unittest
from datetime import datetime
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.stream_cdc_events import CDC_TABLES, split_cdc_events

FIXTURES = json.loads((Path(__file__).parent / "fixtures_debezium_order_lines.json").read_text(encoding="utf-8"))


def kafka_rows(spark, messages):
    """messages: list of (key, value or None). Builds a DataFrame with the Kafka source schema."""
    schema = StructType([
        StructField("key", BinaryType(), True), StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True), StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True), StructField("timestamp", TimestampType(), True),
    ])
    rows = [(bytearray(k.encode()) if k else None, bytearray(v.encode()) if v is not None else None,
             "scm.scm_oms.dbo.order_lines", 0, i, datetime(2026, 10, 15, 13, 0)) for i, (k, v) in enumerate(messages)]
    return spark.createDataFrame(rows, schema=schema)


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestCdcBronze(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestCdcBronze")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        messages = [(FIXTURES[op]["key"], FIXTURES[op]["value"]) for op in ("r", "c", "u", "d", "tombstone")]
        messages.append(('{"order_id":"X","order_line_number":1}', "this is not json"))
        events, dlq = split_cdc_events(kafka_rows(cls.spark, messages))
        cls.events = {r.op: r for r in events.collect()}
        cls.dlq = dlq.collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_every_change_type_lands_and_tombstone_is_skipped(self):
        self.assertEqual(set(self.events), {"r", "c", "u", "d"})

    def test_garbage_goes_to_dead_letter_verbatim(self):
        self.assertEqual(len(self.dlq), 1)
        self.assertEqual(self.dlq[0].payload, "this is not json")
        self.assertEqual(self.dlq[0].dlq_reason, "NOT_A_DEBEZIUM_EVENT")

    def test_row_images_are_kept_as_raw_json(self):
        u = self.events["u"]
        before, after = json.loads(u.before), json.loads(u.after)
        expected = json.loads(FIXTURES["u"]["value"])
        self.assertEqual(before, expected["before"])
        self.assertEqual(after, expected["after"])
        self.assertIsNone(self.events["c"].before)
        self.assertIsNone(self.events["d"].after)
        self.assertEqual(json.loads(self.events["d"].before)["order_line_number"], 2)

    def test_snapshot_flag(self):
        self.assertTrue(self.events["r"].is_snapshot)          # source.snapshot = "first"
        for op in ("c", "u", "d"):
            self.assertFalse(self.events[op].is_snapshot)

    def test_source_metadata(self):
        u = self.events["u"]
        src = json.loads(FIXTURES["u"]["value"])["source"]
        self.assertEqual((u.source_db, u.source_schema, u.source_table), ("scm_oms", "dbo", "order_lines"))
        self.assertEqual((u.commit_lsn, u.change_lsn, u.event_serial_no), (src["commit_lsn"], src["change_lsn"], 2))
        self.assertEqual(u.record_key, FIXTURES["u"]["key"])
        self.assertEqual(u._source_system, "sqlserver_cdc")
        self.assertIsNotNone(u.source_ts)

    def test_source_position_orders_changes_like_the_log(self):
        """Snapshot < the later changes, and the changes sort in captured LSN order."""
        positions = {op: self.events[op].source_position for op in ("r", "u", "c", "d")}
        self.assertEqual(sorted(positions, key=positions.get), ["r", "u", "c", "d"])
        self.assertEqual(positions["r"], json.loads(FIXTURES["r"]["value"])["source"]["commit_lsn"] + "||000000")

    def test_same_event_redelivered_has_same_position(self):
        again, _ = split_cdc_events(kafka_rows(self.spark, [(FIXTURES["u"]["key"], FIXTURES["u"]["value"])]))
        self.assertEqual(again.collect()[0].source_position, self.events["u"].source_position)

    def test_topics_map_to_bronze_tables(self):
        self.assertEqual(CDC_TABLES, {"order_lines": "orders_cdc", "inventory": "inventory_cdc"})


if __name__ == "__main__":
    unittest.main()
