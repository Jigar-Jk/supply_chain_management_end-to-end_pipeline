"""
reconcile_cdc.py – Prove the CDC pipeline: Silver's view of order_lines == SQL Server's, row by row.

Runs inside the Spark container. Applies the Silver CDC logic to bronze.orders_cdc
(decode -> drop redeliveries -> highest source_position per order line) and compares the live
(non-deleted) rows with the SQL Server export written by export_oms_state.py.

Also checks soft deletes: every key Silver marks deleted must be absent from SQL Server.
Bad values the simulator injects are compared like any other value here (this checks the apply,
not the quality gate, which quarantines them separately in fact_order).
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scm_platform.transform_silver import decode_order_line_cdc, latest_cdc_rows

lake = sys.argv[1] if len(sys.argv) > 1 else "/lake"
spark = SparkSession.builder.appName("reconcile_cdc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

KEY = ["order_id", "order_line_number"]
COMPARED = ["customer_id", "product_id", "warehouse_id", "order_status", "ordered_quantity",
            "shipped_quantity", "unit_price"]

bronze = spark.read.format("delta").load(f"{lake}/bronze/orders_cdc")
state = latest_cdc_rows(decode_order_line_cdc(bronze), KEY).cache()
silver_live = state.filter(~F.col("_is_deleted")).select(*KEY, *COMPARED)
silver_deleted = state.filter(F.col("_is_deleted")).select(*KEY)

source = (spark.read.option("header", True).csv(f"{lake}/recon/order_lines_source.csv")
          .select(F.col("order_id"), F.col("order_line_number").cast("int"),
                  F.col("customer_id"), F.col("product_id"), F.col("warehouse_id"), F.col("order_status"),
                  F.col("ordered_quantity").cast("int"), F.col("shipped_quantity").cast("int"),
                  F.round(F.col("unit_price").cast("double"), 2).alias("unit_price")))
silver_live = silver_live.withColumn("unit_price", F.round("unit_price", 2))

joined = silver_live.alias("s").join(source.alias("t"), KEY, "full_outer").cache()
missing = joined.filter(F.col("s.order_status").isNull() & F.col("t.order_status").isNotNull()
                        & F.col("s.customer_id").isNull())
extra = joined.filter(F.col("t.customer_id").isNull() & F.col("s.customer_id").isNotNull())
both = joined.filter(F.col("s.customer_id").isNotNull() & F.col("t.customer_id").isNotNull())
differs = F.lit(False)
for c in COMPARED:
    differs = differs | ~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
mismatched = both.filter(differs)
deleted_but_present = silver_deleted.join(source.select(*KEY), KEY, "inner")

counts = {
    "SQL Server rows": source.count(),
    "Silver live rows": silver_live.count(),
    "Silver soft-deleted rows": silver_deleted.count(),
    "matched exactly": both.count() - mismatched.count(),
    "mismatched values": mismatched.count(),
    "missing in Silver": missing.count(),
    "extra in Silver": extra.count(),
    "deleted in Silver but still in SQL Server": deleted_but_present.count(),
}
print("=" * 64)
print("CDC RECONCILIATION: Silver (from bronze.orders_cdc) vs SQL Server dbo.order_lines")
for name, value in counts.items():
    print(f"  {name:<42} {value:>8}")
ok = all(counts[k] == 0 for k in ("mismatched values", "missing in Silver", "extra in Silver",
                                   "deleted in Silver but still in SQL Server"))
print(f"  RESULT: {'PASS - identical' if ok else 'FAIL'}")
print("=" * 64)
if not ok:
    mismatched.select(*KEY, *[F.col(f"s.{c}").alias(f"silver_{c}") for c in ("order_status", "shipped_quantity")],
                      *[F.col(f"t.{c}").alias(f"sql_{c}") for c in ("order_status", "shipped_quantity")]).show(5, False)
spark.stop()
sys.exit(0 if ok else 1)
