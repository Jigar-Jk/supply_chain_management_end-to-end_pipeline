"""
reprocess_quarantine.py – Close the quarantine loop: PENDING -> REPROCESSED or REJECTED.

Implementation guide §7.5-7.6. Every PENDING quarantine row is replayed through the SAME
transform (and therefore the same quality rules) as the normal pipeline, against the CURRENT
dimensions. One code path, so a retried record never gets a weaker check, and MERGE keeps it
idempotent.

    PENDING --(passes now: late dimension arrived, steward corrected it)--> REPROCESSED (in Silver)
    PENDING --(still failing)--> PENDING (retry_count + 1, failed_rule refreshed)
    PENDING --(still failing past the SLA, default 7 days)--> REJECTED (kept for audit, never in Gold)
    PENDING --(RULE_DUPLICATE_KEY: the kept copy is already in Silver)--> REJECTED
    PENDING --(an older copy of a key that is pending again with a newer version)--> REJECTED (superseded)

Manual correction: a steward writes the fixed record into corrected_payload (original_payload
is never edited, for audit) and the next run replays it.

Not replayed: feeds without an entry in FEEDS (GPS pings, weather). A GPS ping healed hours later
says nothing about where the truck is now, so such rows age out to REJECTED at the SLA.

Order: dimension feeds first, so a corrected warehouse/product can let its facts heal in the same run.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from scm_platform.ingest_bronze import DATASET_SCHEMAS
from scm_platform import transform_silver as ts

PENDING, REPROCESSED, REJECTED = "PENDING", "REPROCESSED", "REJECTED"
DEFAULT_SLA_DAYS = 7
RUN_BY = "reprocess_quarantine"

# Lifecycle columns and feed tagging live in transform_silver (the quality gate writes them)
LIFECYCLE_COLUMNS = ts.QUARANTINE_LIFECYCLE_COLUMNS
with_feed = ts.tag_quarantine_feed


@dataclass(frozen=True)
class FeedSpec:
    """How to replay one feed: its Bronze schema, its transform, and where healed rows go."""
    feed: str
    key_cols: Tuple[str, ...]
    run: Callable[[DataFrame, Dict[str, DataFrame]], Tuple[DataFrame, DataFrame]]
    silver_table: str
    sk_col: str
    freshness_col: Optional[str] = None
    produces_dim: Optional[str] = None      # dimension feeds refresh dims[...] for later feeds


def _dim(transform, name):
    return lambda df, dims: transform(df, dims.get(name))


FEEDS: List[FeedSpec] = [
    # Dimensions first
    FeedSpec("warehouses", ("warehouse_id",), _dim(ts.transform_dim_warehouse, "dim_warehouse"),
             "dim_warehouse", "warehouse_sk", produces_dim="dim_warehouse"),
    FeedSpec("products", ("product_id",), _dim(ts.transform_dim_product, "dim_product"),
             "dim_product", "product_sk", produces_dim="dim_product"),
    FeedSpec("customers", ("customer_id",), _dim(ts.transform_dim_customer, "dim_customer"),
             "dim_customer", "customer_sk", produces_dim="dim_customer"),
    FeedSpec("carriers", ("carrier_id",), _dim(ts.transform_dim_carrier, "dim_carrier"),
             "dim_carrier", "carrier_sk", produces_dim="dim_carrier"),
    FeedSpec("routes", ("route_id",), _dim(ts.transform_dim_route, "dim_route"),
             "dim_route", "route_sk", produces_dim="dim_route"),
    # Facts
    FeedSpec("orders", ("order_id", "order_line_number"),
             lambda df, d: ts.transform_fact_order(df, d["dim_customer"], d["dim_product"], d.get("dim_warehouse")),
             "fact_order", "order_line_sk", freshness_col="updated_at"),
    FeedSpec("inventory", ("warehouse_id", "product_id", "snapshot_date"),
             lambda df, d: ts.transform_fact_inventory_snapshot(df, d["dim_warehouse"], d["dim_product"]),
             "fact_inventory_snapshot", "inventory_snapshot_sk", freshness_col="recorded_at"),
    FeedSpec("shipments", ("shipment_id",),
             lambda df, d: ts.transform_fact_shipment(df, d["dim_carrier"], d["dim_route"],
                                                      df_dim_warehouse=d.get("dim_warehouse")),
             "fact_shipment", "shipment_leg_sk", freshness_col="updated_at"),
]
FEEDS_BY_NAME = {spec.feed: spec for spec in FEEDS}


def _key(cols: Sequence[str], getter=F.col):
    """Composite key string, NULL if any part is missing (so key-less rows never match)."""
    parts = [getter(c).cast("string") for c in cols]
    all_present = parts[0].isNotNull()
    for p in parts[1:]:
        all_present = all_present & p.isNotNull()
    return F.when(all_present, F.concat_ws("\u0001", *parts))


@dataclass
class ReprocessResult:
    quarantine: DataFrame                     # the whole quarantine table, lifecycle updated
    healed: Dict[str, DataFrame] = field(default_factory=dict)   # silver_table -> rows to MERGE
    dims: Dict[str, DataFrame] = field(default_factory=dict)     # dimensions after healing


def reprocess(
    quarantine: DataFrame,
    dims: Dict[str, DataFrame],
    now: Optional[datetime] = None,
    sla_days: int = DEFAULT_SLA_DAYS,
    run_by: str = RUN_BY,
) -> ReprocessResult:
    """Replay PENDING quarantine rows; return the updated quarantine and the Silver rows to MERGE."""
    spark = quarantine.sparkSession
    now = (now or datetime.now(timezone.utc)).replace(tzinfo=None)
    sla_cutoff = now - timedelta(days=sla_days)
    dims = dict(dims)

    pending = quarantine.filter(F.col("status") == PENDING)
    replayable = pending.filter((F.col("failed_rule") != "RULE_DUPLICATE_KEY")
                                & F.col("source_feed").isin(list(FEEDS_BY_NAME)))

    outcomes = []          # DataFrames of (quarantine_id, outcome, new_rule, new_reason)
    healed: Dict[str, DataFrame] = {}
    for spec in FEEDS:
        rows = replayable.filter(F.col("source_feed") == spec.feed)
        if not rows.limit(1).count():
            continue
        payload = F.coalesce(F.col("corrected_payload"), F.col("original_payload"))
        rebuilt = rows.select("quarantine_id", "ingested_at",
                              F.from_json(payload, DATASET_SCHEMAS[spec.feed]()).alias("_r")) \
                      .select("quarantine_id", "ingested_at", "_r.*") \
                      .withColumn("_key", _key(spec.key_cols)).cache()

        # Several pending copies of one key: replay only the newest, close the rest as superseded
        newest_first = Window.partitionBy("_key").orderBy(F.col("ingested_at").desc(), F.col("quarantine_id").desc())
        ranked = rebuilt.withColumn("_rn", F.when(F.col("_key").isNull(), 1).otherwise(F.row_number().over(newest_first)))
        superseded = ranked.filter(F.col("_rn") > 1).select("quarantine_id", F.lit("superseded").alias("outcome"))
        latest = ranked.filter(F.col("_rn") == 1).drop("_rn").cache()

        replay_input = (latest.drop("quarantine_id", "ingested_at", "_key")
                        .withColumn("_ingest_timestamp", ts.utc_lit(now)))
        valid, still_failing = spec.run(replay_input, dims)
        valid = valid.cache()

        valid_keys = valid.select(_key(spec.key_cols).alias("_key")).where("_key IS NOT NULL").distinct()
        healed_ids = latest.join(valid_keys, "_key", "left_semi").select("quarantine_id", F.lit("healed").alias("outcome"))

        # Refresh failed_rule / failure_reason for rows that now fail for a different reason
        refreshed = (still_failing.select(_key(spec.key_cols, lambda c: F.get_json_object("original_payload", f"$.{c}"))
                                          .alias("_key"), "failed_rule", "failure_reason")
                     .where("_key IS NOT NULL").dropDuplicates(["_key"]))
        failing_ids = (latest.join(healed_ids.select("quarantine_id"), "quarantine_id", "left_anti")
                       .join(refreshed, "_key", "left")
                       .select("quarantine_id", F.lit("failing").alias("outcome"),
                               F.col("failed_rule").alias("new_rule"), F.col("failure_reason").alias("new_reason")))

        outcomes += [superseded, healed_ids, failing_ids]
        # Facts: the transform only saw the replayed rows, so its valid output IS the healed set.
        # Dimensions: the output is the whole dimension (existing + healed), MERGEd on the surrogate key.
        healed[spec.silver_table] = valid
        if spec.produces_dim:
            dims[spec.produces_dim] = valid

    if outcomes:
        normalized = [o.select("quarantine_id", "outcome",
                               (o["new_rule"] if "new_rule" in o.columns else F.lit(None).cast("string")).alias("new_rule"),
                               (o["new_reason"] if "new_reason" in o.columns else F.lit(None).cast("string")).alias("new_reason"))
                      for o in outcomes]
        decided = normalized[0]
        for o in normalized[1:]:
            decided = decided.unionByName(o)
    else:
        decided = spark.createDataFrame([], "quarantine_id string, outcome string, new_rule string, new_reason string")

    q = quarantine.join(decided, "quarantine_id", "left")
    is_pending = F.col("status") == PENDING
    outcome = F.col("outcome")
    duplicate = is_pending & (F.col("failed_rule") == "RULE_DUPLICATE_KEY")
    expired = F.col("ingested_at") < ts.utc_lit(sla_cutoff)
    attempted = is_pending & outcome.isNotNull()

    new_status = (F.when(~is_pending, F.col("status"))
                  .when(duplicate, F.lit(REJECTED))
                  .when(outcome == "healed", F.lit(REPROCESSED))
                  .when(outcome == "superseded", F.lit(REJECTED))
                  .when(expired, F.lit(REJECTED))
                  .otherwise(F.lit(PENDING)))
    note = (F.when(~is_pending, F.col("resolution_note"))
            .when(duplicate, F.lit("Duplicate: the kept copy of this record is already in Silver"))
            .when(outcome == "healed", F.coalesce(F.col("resolution_note"), F.lit("Auto-healed: passed the quality gate on retry")))
            .when(outcome == "superseded", F.lit("Superseded by a newer pending version of the same record"))
            .when(expired, F.lit(f"Auto-rejected: exceeded {sla_days}-day quarantine SLA"))
            .otherwise(F.col("resolution_note")))
    closing = is_pending & (new_status != PENDING)

    updated = q.select(
        "quarantine_id", "original_payload",
        F.when(attempted & (outcome == "failing"), F.coalesce("new_reason", "failure_reason"))
         .otherwise(F.col("failure_reason")).alias("failure_reason"),
        F.when(attempted & (outcome == "failing"), F.coalesce("new_rule", "failed_rule"))
         .otherwise(F.col("failed_rule")).alias("failed_rule"),
        "batch_id", "source_system", "ingested_at",
        new_status.alias("status"),
        "source_feed", "corrected_payload",
        (F.col("retry_count") + F.when(attempted, 1).otherwise(0)).cast("int").alias("retry_count"),
        F.when(attempted, ts.utc_lit(now)).otherwise(F.col("last_retry_at")).alias("last_retry_at"),
        F.when(closing, ts.utc_lit(now)).otherwise(F.col("resolved_at")).alias("resolved_at"),
        F.when(closing, F.coalesce(F.col("resolved_by"), F.lit(run_by))).otherwise(F.col("resolved_by")).alias("resolved_by"),
        note.alias("resolution_note"),
    )
    return ReprocessResult(quarantine=updated, healed=healed, dims=dims)


def summarize(before: DataFrame, after: DataFrame) -> List[tuple]:
    """(failed_rule, status_before, status_after, rows) for the run report."""
    joined = before.select("quarantine_id", "failed_rule", F.col("status").alias("before")) \
        .join(after.select("quarantine_id", F.col("status").alias("after")), "quarantine_id")
    return [tuple(r) for r in joined.groupBy("failed_rule", "before", "after").count()
            .orderBy("failed_rule", "after").collect()]


def update_quarantine_lifecycle(spark: SparkSession, table_name: str, updated: DataFrame) -> None:
    """Databricks: MERGE the new lifecycle state into quarantine.records (only rows that changed)."""
    from delta.tables import DeltaTable
    (DeltaTable.forName(spark, table_name).alias("t")
     .merge(updated.alias("s"), "t.quarantine_id = s.quarantine_id")
     .whenMatchedUpdate(
         condition="t.status <> s.status OR NOT (t.retry_count <=> s.retry_count) OR NOT (t.failed_rule <=> s.failed_rule)",
         set={c: f"s.{c}" for c in ("status", "failed_rule", "failure_reason", "retry_count", "last_retry_at",
                                    "resolved_at", "resolved_by", "resolution_note")})
     .execute())


def run_reprocessing(spark: SparkSession, catalog: str = "scm_platform_dev") -> None:
    """Databricks job entrypoint (scheduled every 2 hours): replay, MERGE healed rows, update lifecycle."""
    quarantine_table = f"{catalog}.quarantine.records"
    dims = {name: spark.table(f"{catalog}.silver.{name}")
            for name in ("dim_warehouse", "dim_product", "dim_customer", "dim_carrier", "dim_route")
            if spark.catalog.tableExists(f"{catalog}.silver.{name}")}
    result = reprocess(spark.table(quarantine_table), dims)
    for spec in FEEDS:
        if spec.silver_table in result.healed:
            ts.write_silver_table(result.healed[spec.silver_table], f"{catalog}.silver.{spec.silver_table}",
                                  key_col=spec.sk_col, spark=spark, freshness_col=spec.freshness_col)
    update_quarantine_lifecycle(spark, quarantine_table, result.quarantine)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Replay PENDING quarantine rows through the quality gate.")
    parser.add_argument("--catalog", default="scm_platform_dev")
    args, _ = parser.parse_known_args()
    run_reprocessing(SparkSession.builder.getOrCreate(), args.catalog)


if __name__ == "__main__":
    main()
