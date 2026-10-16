"""Print CDC Bronze row counts by table and op, plus samples (run inside the cdc-stream-inspect container)."""

import sys

from pyspark.sql import SparkSession

lake = sys.argv[1] if len(sys.argv) > 1 else "/lake"
spark = SparkSession.builder.appName("inspect_cdc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 78)
for table in ("orders_cdc", "inventory_cdc"):
    df = spark.read.format("delta").load(f"{lake}/bronze/{table}")
    by_op = {r.op: r["count"] for r in df.groupBy("op").count().collect()}
    print(f"bronze.{table:<14} rows: {df.count():>7}   by op: {dict(sorted(by_op.items()))}   "
          f"snapshot rows: {df.filter('is_snapshot').count()}")
try:
    print(f"bronze.cdc_dlq         rows: {spark.read.format('delta').load(f'{lake}/bronze/cdc_dlq').count():>7}")
except Exception:
    print("bronze.cdc_dlq         rows:       0 (table not created yet)")
print("=" * 78)
spark.read.format("delta").load(f"{lake}/bronze/orders_cdc").filter("op <> 'r'") \
    .orderBy("source_position").select("op", "record_key", "source_position", "source_ts") \
    .show(5, truncate=False)
spark.stop()
