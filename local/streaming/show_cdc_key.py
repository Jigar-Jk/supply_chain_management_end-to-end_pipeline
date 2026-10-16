"""Show one order line's CDC events in Bronze (in arrival order) and the state Silver applies.

Usage inside the Spark container:  show_cdc_key.py /lake ORDER_ID LINE_NUMBER
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scm_platform.transform_silver import decode_order_line_cdc, latest_cdc_rows

lake, order_id, line_no = sys.argv[1], sys.argv[2], int(sys.argv[3])
spark = SparkSession.builder.appName("show_cdc_key").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

decoded = decode_order_line_cdc(spark.read.format("delta").load(f"{lake}/bronze/orders_cdc")
                                .withColumn("arrival", F.concat_ws("/", "kafka_partition", "kafka_offset")))
mine = decoded.filter((F.col("order_id") == order_id) & (F.col("order_line_number") == line_no))
print(f"=== Bronze events for {order_id} line {line_no}, in ARRIVAL order (Kafka offset) ===")
(spark.read.format("delta").load(f"{lake}/bronze/orders_cdc")
 .filter(F.get_json_object("record_key", "$.order_id") == order_id)
 .filter(F.get_json_object("record_key", "$.order_line_number").cast("int") == line_no)
 .select("kafka_offset", "op", F.get_json_object("after", "$.order_status").alias("status_after"), "source_position")
 .orderBy("kafka_offset").show(truncate=False))
print("=== State Silver applies (highest source_position wins) ===")
latest_cdc_rows(mine, ["order_id", "order_line_number"]).select(
    "order_id", "order_line_number", "order_status", "shipped_quantity", "_source_position").show(truncate=False)
spark.stop()
