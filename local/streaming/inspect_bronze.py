"""Print row counts and samples of the local Bronze GPS tables (run inside the gps-stream-inspect container)."""

import sys

from pyspark.sql import SparkSession

lake = sys.argv[1] if len(sys.argv) > 1 else "/lake"
spark = SparkSession.builder.appName("inspect_bronze").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

events = spark.read.format("delta").load(f"{lake}/bronze/gps_events")
dlq = spark.read.format("delta").load(f"{lake}/bronze/gps_events_dlq")

print("=" * 70)
print(f"bronze.gps_events      rows: {events.count()}")
print(f"  distinct event_id        : {events.select('event_id').distinct().count()}")
print(f"  lat out of range         : {events.filter('lat NOT BETWEEN -90 AND 90').count()}")
print(f"  missing truck_id         : {events.filter('truck_id IS NULL').count()}")
print(f"  trucks / shipments       : {events.select('truck_id').distinct().count()} / "
      f"{events.select('shipment_id').distinct().count()}")
print(f"bronze.gps_events_dlq  rows: {dlq.count()}")
print("=" * 70)
events.select("event_id", "event_type", "truck_id", "shipment_id", "lat", "lon", "event_ts", "kafka_partition",
              "kafka_offset").show(3, truncate=False)
dlq.select("payload", "dlq_reason", "kafka_offset").show(3, truncate=False)
spark.stop()
