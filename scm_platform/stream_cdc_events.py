"""
stream_cdc_events.py – CDC ingestion: Debezium change events from Kafka / Event Hubs into Bronze.

Pattern 3 of the implementation guide (§6.3), with SQL Server as the OMS source:
    SQL Server CDC -> Debezium (Kafka Connect) -> topics scm.scm_oms.dbo.<table>
      -> this job -> bronze.<table>_cdc     one row per change event (append-only)
                  -> bronze.cdc_dlq         messages that are not JSON objects (kept verbatim)

Bronze is generic across tables: it stores the Debezium envelope as columns and the row images
(before / after) as raw JSON text, so Bronze never breaks when a source column is added; Silver
applies each table's schema.

source_position orders every change: commit LSN, then change LSN, then event serial number, as
one fixed-width string, so "greater string = later change". Silver keeps the highest position
per key, which makes it immune to out-of-order delivery and to Debezium re-sending events after
a crash (a re-sent event has the same position). So Bronze does not deduplicate CDC.

Tombstones (the null-value message Debezium sends after a delete, for log compaction) carry no
data and are skipped.

Run (one streaming query per table, each with its own checkpoint):
    Databricks: --catalog scm_platform_dev --checkpoint-root abfss://checkpoints@.../cdc
    Local:      --lake-path /lake
"""

import argparse
import os
from dataclasses import replace
from typing import Any, Dict, Optional, Tuple

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, concat, current_timestamp, expr, lit, lpad
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.stream_gps_events import StreamConfig, kafka_options, write_stream

TOPIC_PREFIX = "scm.scm_oms.dbo"
# Debezium topic per source table -> Bronze table name
CDC_TABLES: Dict[str, str] = {
    "order_lines": "orders_cdc",
    "inventory": "inventory_cdc",
}
DLQ_TABLE = "cdc_dlq"
SOURCE_SYSTEM = "sqlserver_cdc"


def _json(path: str):
    return expr(f"get_json_object(payload, '{path}')")


def split_cdc_events(raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split Kafka records from a Debezium topic into (change_events, dead_letters).

    Works on batch or streaming frames. Tombstones (null value) are dropped; other messages that
    are not JSON objects are dead-lettered verbatim.
    """
    base = raw.select(
        col("key").cast("string").alias("record_key"),
        col("value").cast("string").alias("payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).filter(col("payload").isNotNull())                          # tombstones
    base = base.withColumn("_is_json", expr("coalesce(substring(trim(payload), 1, 1) = '{' "
                                            "AND get_json_object(payload, '$.op') IS NOT NULL, false)"))

    dead_letters = base.filter(~col("_is_json")).drop("_is_json").withColumns({
        "dlq_reason": lit("NOT_A_DEBEZIUM_EVENT"),
        "_ingest_timestamp": current_timestamp(),
        "_source_system": lit(SOURCE_SYSTEM),
    })

    serial = expr("coalesce(cast(get_json_object(payload, '$.source.event_serial_no') AS INT), 0)")
    events = (
        base.filter(col("_is_json"))
        .select(
            "record_key",
            _json("$.op").alias("op"),
            _json("$.before").alias("before"),
            _json("$.after").alias("after"),
            _json("$.source.db").alias("source_db"),
            _json("$.source.schema").alias("source_schema"),
            _json("$.source.table").alias("source_table"),
            _json("$.source.commit_lsn").alias("commit_lsn"),
            _json("$.source.change_lsn").alias("change_lsn"),
            serial.alias("event_serial_no"),
            expr("coalesce(get_json_object(payload, '$.source.snapshot'), 'false') <> 'false'").alias("is_snapshot"),
            expr("timestamp_millis(cast(get_json_object(payload, '$.source.ts_ms') AS BIGINT))").alias("source_ts"),
            expr("timestamp_millis(cast(get_json_object(payload, '$.ts_ms') AS BIGINT))").alias("captured_ts"),
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        )
        # LSNs are fixed-width hex ("00000033:00006230:0003"), so string order = log order.
        # Snapshot rows have no change_lsn: '' sorts before any real change at the same commit LSN.
        .withColumn("source_position", concat(
            col("commit_lsn"), lit("|"), expr("coalesce(change_lsn, '')"), lit("|"),
            lpad(col("event_serial_no").cast("string"), 6, "0")))
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
    )
    return events, dead_letters


def ensure_table(spark: SparkSession, cfg: StreamConfig, name: str, schema) -> None:
    """Create an empty Delta target if it does not exist yet.

    Several queries append to bronze.cdc_dlq. Concurrent appends to an existing Delta table are
    fine, but two queries racing to CREATE it fail (DELTA_PROTOCOL_CHANGED), so shared targets are
    created once up front. On Databricks Liquibase (migration 011) has normally created it already.
    """
    kind, location = cfg.target(name)
    empty = spark.createDataFrame([], schema)
    if kind == "table":
        if not spark.catalog.tableExists(location):
            empty.write.format("delta").mode("append").saveAsTable(location)
    else:
        empty.write.format("delta").mode("append").save(location)   # no-op on an existing table


def start_cdc_streams(spark: SparkSession, cfg: StreamConfig, connection_string: Optional[str] = None):
    """One query per source table into bronze.<table>_cdc, plus its dead-letter query; returns all handles."""
    streams = []
    for source_table, bronze_table in CDC_TABLES.items():
        topic_cfg = replace(cfg, topic=f"{TOPIC_PREFIX}.{source_table}")
        raw = spark.readStream.format("kafka").options(**kafka_options(topic_cfg, connection_string)).load()
        streams.append((source_table, bronze_table, *split_cdc_events(raw)))

    ensure_table(spark, cfg, DLQ_TABLE, streams[0][3].schema)
    queries = []
    for source_table, bronze_table, events, dead_letters in streams:
        queries.append(write_stream(events, cfg, bronze_table))
        queries.append(write_stream(dead_letters, cfg, DLQ_TABLE, checkpoint_name=f"{DLQ_TABLE}_{source_table}"))
    return queries


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Debezium CDC events from Kafka / Event Hubs into Bronze.")
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--catalog", default=None, help="Databricks: Unity Catalog catalog name")
    parser.add_argument("--lake-path", default=None, help="Local: root folder for Delta tables")
    parser.add_argument("--checkpoint-root", default=None)
    parser.add_argument("--available-now", action="store_true", help="Process what is available, then stop")
    args, _ = parser.parse_known_args()

    root = args.checkpoint_root or (f"{args.lake_path.rstrip('/')}/_checkpoints" if args.lake_path else None)
    if not root:
        parser.error("--checkpoint-root is required with --catalog")
    cfg = StreamConfig(bootstrap_servers=args.bootstrap, checkpoint_root=root, catalog=args.catalog,
                       lake_path=args.lake_path, available_now=args.available_now)

    spark = SparkSession.builder.appName("stream_cdc_events").getOrCreate()
    for query in start_cdc_streams(spark, cfg, os.environ.get("EVENTHUB_CONNECTION_STRING")):
        query.awaitTermination()


if __name__ == "__main__":
    main()
