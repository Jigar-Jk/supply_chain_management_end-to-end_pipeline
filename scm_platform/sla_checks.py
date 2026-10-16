"""
sla_checks.py – WMS nightly file SLA and the last-known-good fallback (guide §6.2, failure scenario 2).

The WMS stock reconciliation for night D is due by ops.sla_config.expected_by_time (02:00 UTC).
- check_wms_snapshot: per warehouse, did night D's snapshot arrive, and on time?
    ON_TIME   arrived before the deadline
    LATE      arrived after the deadline
    SLA_MISS  past the deadline and still missing  -> alert + carry forward
    WAITING   missing, deadline not reached yet
- carry_forward_last_good: for every warehouse missing night D, copy its latest earlier snapshot
  as night D with is_stale = TRUE, so stockout rate / turnover still compute and Gold reports
  built_from_stale_data = TRUE. When the late file finally arrives, the normal MERGE replaces those
  rows (same key, newer recorded_at), so no manual cleanup is needed.
"""

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

ON_TIME, LATE, SLA_MISS, WAITING = "ON_TIME", "LATE", "SLA_MISS", "WAITING"
DEFAULT_EXPECTED_BY = "02:00"


@dataclass
class SnapshotStatus:
    snapshot_date: date
    deadline: datetime
    status: str                                   # worst status across warehouses
    by_warehouse: Dict[str, str]                  # warehouse_id -> status
    missing_warehouses: List[str]

    @property
    def message(self) -> str:
        if self.status == SLA_MISS:
            return (f"WMS snapshot {self.snapshot_date} missing for {len(self.missing_warehouses)} warehouse(s) "
                    f"after the {self.deadline:%H:%M} UTC deadline; Silver carries the last good snapshot forward")
        if self.status == LATE:
            late = sorted(w for w, s in self.by_warehouse.items() if s == LATE)
            return f"WMS snapshot {self.snapshot_date} arrived after the deadline for {len(late)} warehouse(s)"
        return f"WMS snapshot {self.snapshot_date}: {self.status}"


def deadline_for(snapshot_date: date, expected_by: Optional[str]) -> datetime:
    hh, mm = (expected_by or DEFAULT_EXPECTED_BY).split(":")
    return datetime.combine(snapshot_date, time(int(hh), int(mm)))


def check_wms_snapshot(
    bronze_inventory: DataFrame,
    warehouses: List[str],
    snapshot_date: date,
    now: datetime,
    expected_by: Optional[str] = DEFAULT_EXPECTED_BY,
) -> SnapshotStatus:
    """Arrival status of night snapshot_date for each expected warehouse.

    Arrival time = earliest _ingest_timestamp of that warehouse's rows for the night.
    `now` is naive (or aware) UTC; the deadline is applied in UTC regardless of the session zone.
    """
    now = now.replace(tzinfo=None) if now.tzinfo else now
    deadline = deadline_for(snapshot_date, expected_by)
    # Compare inside Spark against explicit UTC strings: collecting timestamps into Python would
    # convert them to the driver's local zone and could misclassify an on-time file as LATE.
    deadline_ts = F.to_utc_timestamp(F.lit(deadline.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"),
                                     F.current_timezone())
    arrived = {r.warehouse_id: r.late for r in
               bronze_inventory.filter(F.col("snapshot_date") == F.lit(snapshot_date.isoformat()))
               .groupBy("warehouse_id").agg((F.min("_ingest_timestamp") > deadline_ts).alias("late")).collect()}
    by_warehouse = {}
    for wh in warehouses:
        if wh in arrived:
            by_warehouse[wh] = LATE if arrived[wh] else ON_TIME
        else:
            by_warehouse[wh] = SLA_MISS if now > deadline else WAITING
    order = [SLA_MISS, LATE, WAITING, ON_TIME]
    worst = min(by_warehouse.values(), key=order.index) if by_warehouse else ON_TIME
    missing = sorted(w for w, s in by_warehouse.items() if s in (SLA_MISS, WAITING))
    return SnapshotStatus(snapshot_date, deadline, worst, by_warehouse, missing)


def carry_forward_last_good(fact_inventory: DataFrame, snapshot_date: date, warehouses: List[str]) -> DataFrame:
    """Rows for night snapshot_date copied from each listed warehouse's latest earlier night, is_stale = TRUE."""
    if not warehouses:
        return fact_inventory.limit(0)
    earlier = fact_inventory.filter((F.col("snapshot_date") < F.lit(snapshot_date)) & F.col("warehouse_id").isin(warehouses))
    latest = Window.partitionBy("warehouse_id")
    last_good = (earlier.withColumn("_latest", F.max("snapshot_date").over(latest))
                 .filter(F.col("snapshot_date") == F.col("_latest")).drop("_latest"))
    date_sk = int(snapshot_date.strftime("%Y%m%d"))
    return (last_good
            .withColumn("snapshot_date", F.lit(snapshot_date))
            .withColumn("snapshot_date_sk", F.lit(date_sk))
            .withColumn("inventory_snapshot_sk",
                        F.md5(F.concat_ws("_", F.col("warehouse_id"), F.col("product_id"), F.lit(str(date_sk)))))
            .withColumn("is_stale", F.lit(True))
            .withColumn("created_at", F.current_timestamp())
            .select(*fact_inventory.columns))


def run_log_row(status: SnapshotStatus, run_id: str, started_at: datetime, rows_carried: int) -> tuple:
    """A row for ops.pipeline_run_log (columns as in migration 013)."""
    return (run_id, "wms_batch", "check_wms_sla", "bronze", started_at, datetime.now(timezone.utc).replace(tzinfo=None),
            None, rows_carried, 0, "SLA_MISS" if status.status == SLA_MISS else "SUCCESS", status.message)


def run_wms_sla_check(spark: SparkSession, catalog: str = "scm_platform_dev",
                      snapshot_date: Optional[date] = None, run_id: str = "manual") -> SnapshotStatus:
    """Databricks task, scheduled just after the deadline: check, carry forward, log, alert on a miss."""
    from scm_platform.transform_silver import write_silver_table

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    snapshot_date = snapshot_date or started.date()
    sla = spark.table(f"{catalog}.ops.sla_config").filter("source_system = 'wms_batch'").first()
    warehouses = [r.warehouse_id for r in spark.table(f"{catalog}.silver.dim_warehouse").filter("is_current").collect()]
    status = check_wms_snapshot(spark.table(f"{catalog}.bronze.inventory"), warehouses, snapshot_date, started,
                                sla.expected_by_time if sla else DEFAULT_EXPECTED_BY)
    carried = 0
    if status.status == SLA_MISS:
        stale = carry_forward_last_good(spark.table(f"{catalog}.silver.fact_inventory_snapshot"),
                                        snapshot_date, status.missing_warehouses)
        carried = stale.count()
        write_silver_table(stale, f"{catalog}.silver.fact_inventory_snapshot", key_col="inventory_snapshot_sk",
                           spark=spark, freshness_col="recorded_at")
    spark.createDataFrame([run_log_row(status, run_id, started, carried)],
                          spark.table(f"{catalog}.ops.pipeline_run_log").schema) \
        .write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_run_log")
    if status.status == SLA_MISS:
        raise RuntimeError(f"SLA miss: {status.message}")      # fails the task -> Workflow alert fires
    return status


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="WMS nightly snapshot SLA check with last-known-good fallback.")
    parser.add_argument("--catalog", default="scm_platform_dev")
    parser.add_argument("--snapshot-date", type=date.fromisoformat, default=None)
    parser.add_argument("--run-id", default="manual")
    args, _ = parser.parse_known_args()
    run_wms_sla_check(SparkSession.builder.getOrCreate(), args.catalog, args.snapshot_date, args.run_id)


if __name__ == "__main__":
    main()
