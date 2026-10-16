"""
demo_reprocessing.py – Watch the quarantine reprocessing job in SSMS: before -> after.

On the full dev data, builds a realistic "before" state and runs the real reprocessing job:
  1. Late dimension: the busiest warehouse is missing from dim_warehouse when its orders arrive,
     so its order lines are quarantined (RULE_REF_INTEGRITY_WAREHOUSE). Then it arrives.
  2. Steward correction: one zero-quantity line gets a corrected_payload (qty 5).
  3. SLA: a few unit-price failures are 8 days old.
Publishes to scm_preview:
  quarantine.records_before_reprocess / quarantine.records_after_reprocess
  silver.fact_order_before_reprocess  / silver.fact_order_after_reprocess
  ops.reprocess_summary  (failed_rule, status before -> after, rows)

Usage (SQL Server running):  python -m local.preview.demo_reprocessing
"""

import json
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scm_platform import reprocess_quarantine as rq
from scm_platform import transform_silver as ts
from local.preview.run_local_pipeline import DATA, DATABASE, QUARANTINE_COLUMNS, Run
from local.preview.sqlserver_sink import connect, ensure_database, write_rows, write_table


def main() -> None:
    t0 = time.monotonic()
    spark = (SparkSession.builder.master("local[2]").appName("demo_reprocessing")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    run = Run(spark)
    now = datetime.now(timezone.utc)

    b = {feed: run.bronze(feed, [DATA / folder / f"{folder}.jsonl"]) for feed, folder in
         [("warehouses", "warehouses"), ("products", "products"), ("customers", "customers"), ("orders", "orders")]}
    full = {
        "dim_warehouse": ts.transform_dim_warehouse(b["warehouses"])[0].cache(),
        "dim_product": ts.transform_dim_product(b["products"])[0].cache(),
        "dim_customer": ts.transform_dim_customer(b["customers"])[0].cache(),
    }

    # 1. The busiest warehouse "arrives late": absent from the dimension on the first run
    late = (b["orders"].filter(F.col("warehouse_id").startswith("WH-0")).groupBy("warehouse_id").count()
            .orderBy(F.desc("count"), "warehouse_id").first().warehouse_id)
    early_dims = dict(full, dim_warehouse=full["dim_warehouse"].filter(F.col("warehouse_id") != late))
    fact_before, quar = ts.transform_fact_order(b["orders"], early_dims["dim_customer"], early_dims["dim_product"],
                                                early_dims["dim_warehouse"], batch_id=run.batch_id)
    q = ts.tag_quarantine_feed(quar, "orders").cache()

    # 2. Steward correction on one zero-quantity line; 3. a few unit-price failures are 8 days old
    qty_row = q.filter("failed_rule = 'RULE_ORDER_QUANTITY_POSITIVE'").orderBy("quarantine_id").first()
    fixed = json.loads(qty_row.original_payload)
    fixed["ordered_quantity"] = 5
    old_ids = [r.quarantine_id for r in q.filter("failed_rule = 'RULE_UNIT_PRICE_NON_NEGATIVE'")
               .orderBy("quarantine_id").limit(10).collect()]
    is_fixed = F.col("quarantine_id") == qty_row.quarantine_id
    naive_now = now.replace(tzinfo=None)
    before = (q.withColumn("corrected_payload", F.when(is_fixed, F.lit(json.dumps(fixed))).otherwise(F.col("corrected_payload")))
               .withColumn("resolved_by", F.when(is_fixed, F.lit("steward@scm")).otherwise(F.col("resolved_by")))
               .withColumn("resolution_note", F.when(is_fixed, F.lit("Typo in source quantity, confirmed with OMS team"))
                           .otherwise(F.col("resolution_note")))
               .withColumn("ingested_at", F.when(F.col("quarantine_id").isin(old_ids), ts.utc_lit(naive_now - timedelta(days=8)))
                           .otherwise(ts.utc_lit(naive_now - timedelta(hours=2))))
               .select(*QUARANTINE_COLUMNS))
    before = spark.createDataFrame(before.collect(), before.schema).cache()    # as stored in the table

    # Run the real job against the dimensions as they are NOW (late warehouse has arrived)
    result = rq.reprocess(before, full, now=now)
    after = spark.createDataFrame(result.quarantine.collect(), result.quarantine.schema).cache()
    healed = result.healed["fact_order"].cache()
    fact_after = fact_before.unionByName(healed.select(*fact_before.columns))
    summary = rq.summarize(before, after)
    counts = {"fact_before": fact_before.count(), "healed": healed.count(), "fact_after": fact_after.count()}

    ensure_database(DATABASE)
    with connect(DATABASE) as conn:
        for schema, table, df in [("quarantine", "records_before_reprocess", before),
                                  ("quarantine", "records_after_reprocess", after),
                                  ("silver", "fact_order_before_reprocess", fact_before),
                                  ("silver", "fact_order_after_reprocess", fact_after)]:
            print(f"  [{schema}].[{table}] {write_table(conn, df, schema, table)} rows")
        write_rows(conn, "ops", "reprocess_summary",
                   [("failed_rule", "NVARCHAR(60)"), ("status_before", "NVARCHAR(20)"),
                    ("status_after", "NVARCHAR(20)"), ("rows", "INT")], summary)
    spark.stop()

    print(f"\nLate warehouse: {late}   steward-corrected line: {fixed['order_id']} line {fixed['order_line_number']}")
    print(f"fact_order rows: before {counts['fact_before']} + healed {counts['healed']} = after {counts['fact_after']}")
    print(f"{'failed_rule':<34} {'before':<9} -> {'after':<11} rows")
    for rule, s_before, s_after, n in summary:
        print(f"{rule:<34} {s_before:<9} -> {s_after:<11} {n}")
    print(f"Done in {time.monotonic() - t0:.0f}s. In SSMS: SELECT * FROM {DATABASE}.ops.reprocess_summary")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
