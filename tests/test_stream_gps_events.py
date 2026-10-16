"""
test_stream_gps_events.py – GPS/RFID streaming path without a broker.

The streaming job's parsing (split_events) and dedup (dedup_events) are plain DataFrame
functions, so they are tested on a batch DataFrame shaped like Kafka records, fed with the
producer's real messages. The Silver GPS gate and the latest-ping join are tested the same way.
The live broker path is exercised by local/streaming/docker-compose.yml.

Run locally with:
    python -m unittest tests.test_stream_gps_events -v
"""

import json
import unittest
from collections import Counter
from datetime import timedelta, timezone

from data_generators.produce_gps import build_fleet, generate_messages
from tests.test_produce_gps import NOW, build_inputs, fixed_clock

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from scm_platform.stream_gps_events import StreamConfig, dedup_events, kafka_options, split_events
    from scm_platform.transform_silver import latest_ping_per_shipment, transform_gps_events

EVENTS = 6000

EXPECTED_GPS_RULE = {
    "lat_out_of_range": "RULE_GPS_COORDINATES",
    "missing_truck_id": "RULE_GPS_TRUCK_ID_MISSING",
}


def as_utc(ts):
    """PySpark returns naive timestamps in the machine's local zone; convert to aware UTC."""
    return ts.astimezone(timezone.utc)


def kafka_frame(spark, messages, partitions=4):
    """Batch DataFrame with the Kafka source schema (value is binary, as the connector delivers it)."""
    schema = StructType([
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
    ])
    base = NOW.replace(tzinfo=None)
    rows = [(None, bytearray(m.encode("utf-8")), "gps-events", i % partitions, i // partitions,
             base + timedelta(milliseconds=i)) for i, m in enumerate(messages)]
    return spark.createDataFrame(rows, schema=schema)


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestGpsStreaming(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("UnitTestGpsStreaming")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")
        feeds, shipments, damaged = build_inputs()
        cls.fleet = build_fleet(shipments, feeds["routes"], damaged, seed=42)
        cls.manifest = []
        cls.messages = list(generate_messages(build_fleet(shipments, feeds["routes"], damaged, seed=42),
                                              EVENTS, seed=42, clock=fixed_clock, manifest=cls.manifest))
        events, dlq = split_events(kafka_frame(cls.spark, cls.messages))
        cls.events_df = dedup_events(events).cache()
        cls.events = cls.events_df.collect()
        cls.dlq = dlq.collect()
        clean, quarantine = transform_gps_events(cls.events_df, batch_id="b_gps")
        cls.clean = clean.cache()
        cls.quarantine = quarantine.collect()
        cls.pings = {r._ping_shipment_id: r for r in latest_ping_per_shipment(cls.clean).collect()}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------ Bronze parsing

    def test_unparseable_messages_go_to_the_dead_letter_table_verbatim(self):
        expected = [m for m in self.messages if m.startswith("{not-json")]
        self.assertEqual(sorted(r.payload for r in self.dlq), sorted(expected))
        for r in self.dlq:
            self.assertEqual(r.dlq_reason, "UNPARSEABLE_JSON")
            self.assertIsNotNone(r.kafka_offset)

    def test_bronze_keeps_every_parseable_event_once(self):
        parseable_ids = {json.loads(m)["event_id"] for m in self.messages if not m.startswith("{not-json")}
        ids = [r.event_id for r in self.events]
        self.assertEqual(len(ids), len(set(ids)), "re-delivered events must be deduplicated")
        self.assertEqual(set(ids), parseable_ids)
        # Bad-but-parseable events are kept in Bronze for the Silver gate to judge
        self.assertTrue(any(r.lat == 999.0 for r in self.events))
        self.assertTrue(any(r.truck_id is None for r in self.events))

    def test_conservation_messages_equal_bronze_plus_dlq_plus_redeliveries(self):
        redeliveries = sum(1 for m in self.manifest if m["failure_mode"] == "duplicate")
        self.assertEqual(len(self.messages), len(self.events) + len(self.dlq) + redeliveries)

    def test_bronze_columns_and_event_time(self):
        r = self.events[0]
        for c in ("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
                  "event_ts", "_ingest_timestamp", "_source_system"):
            self.assertIn(c, r.asDict())
        self.assertEqual(r._source_system, "gps_stream")
        self.assertIsNotNone(r.event_ts)

    # ------------------------------------------------------------------ Silver gate

    def test_bad_events_are_quarantined_under_their_rule(self):
        expected = Counter(EXPECTED_GPS_RULE[m["failure_mode"]] for m in self.manifest
                           if m["failure_mode"] in EXPECTED_GPS_RULE)
        self.assertEqual(Counter(q.failed_rule for q in self.quarantine), expected)
        for q in self.quarantine:
            payload = json.loads(q.original_payload)
            self.assertNotIn("kafka_offset", payload, "payload is the event, not transport metadata")
            self.assertEqual(q.source_system, "GPS_STREAM")

    def test_clean_plus_quarantine_equals_bronze(self):
        self.assertEqual(self.clean.count() + len(self.quarantine), len(self.events))

    def test_latest_ping_per_shipment(self):
        latest = {}
        for m in self.messages:
            if m.startswith("{not-json"):
                continue
            e = json.loads(m)
            if e.get("truck_id") is None or e["lat"] == 999.0:
                continue
            if e["shipment_id"] not in latest or e["ts"] > latest[e["shipment_id"]]["ts"]:
                latest[e["shipment_id"]] = e
        self.assertEqual(set(self.pings), set(latest))
        for sid, e in latest.items():
            p = self.pings[sid]
            self.assertEqual(p.truck_id, e["truck_id"])
            self.assertEqual(as_utc(p.last_ping_ts).strftime("%Y-%m-%dT%H:%M:%S"), e["ts"][:19])

    def test_dark_trucks_have_stale_last_ping(self):
        dark = {t["shipment_id"] for t in self.fleet if t["dark"]}
        cutoff = NOW - timedelta(hours=2)
        for sid, p in self.pings.items():
            self.assertEqual(as_utc(p.last_ping_ts) < cutoff, sid in dark, sid)

    # ------------------------------------------------------------------ configuration

    def test_kafka_options_local_and_event_hubs(self):
        local = kafka_options(StreamConfig("kafka:29092", lake_path="/lake"))
        self.assertEqual(local["kafka.bootstrap.servers"], "kafka:29092")
        self.assertNotIn("kafka.security.protocol", local)
        dbx = kafka_options(StreamConfig("ehns.servicebus.windows.net:9093", catalog="scm_platform_dev"),
                            connection_string="Endpoint=sb://ehns/;SharedAccessKey=k")
        self.assertEqual(dbx["kafka.security.protocol"], "SASL_SSL")
        self.assertIn("kafkashaded.org.apache.kafka", dbx["kafka.sasl.jaas.config"])
        self.assertIn('username="$ConnectionString"', dbx["kafka.sasl.jaas.config"])

    def test_targets(self):
        self.assertEqual(StreamConfig("b", catalog="scm_platform_dev").target("gps_events"),
                         ("table", "scm_platform_dev.bronze.gps_events"))
        self.assertEqual(StreamConfig("b", lake_path="/lake/").target("gps_events_dlq"),
                         ("path", "/lake/bronze/gps_events_dlq"))
        with self.assertRaises(ValueError):
            StreamConfig("b").target("gps_events")


if __name__ == "__main__":
    unittest.main()
