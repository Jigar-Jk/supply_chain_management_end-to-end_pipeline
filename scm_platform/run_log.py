"""
run_log.py – Record what a job step did in ops.pipeline_run_log (migration 013).

The pipeline SLA tile (dashboards/dq/04_pipeline_sla_status.sql) reads the last SUCCESS per source
system from this table, so every step that brings a source's data in logs one row when it finishes:

    transform_silver   fact_order -> sqlserver_cdc · fact_shipment -> carrier_api ·
                       fact_inventory_snapshot -> wms_batch
    stream_gps_events  one drain of the GPS topic -> gps_stream
    stream_cdc_events  one drain of the CDC topics -> sqlserver_cdc
    sla_checks         the WMS file check -> wms_batch (SUCCESS or SLA_MISS)

Logging never breaks a job: without the table (local runs into a lake path, unit tests) it is skipped.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Tuple

log = logging.getLogger(__name__)

SUCCESS = "SUCCESS"


def utc_now() -> datetime:
    """Naive UTC, like every timestamp the pipeline writes."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_log_row(run_id: str, source_system: str, step: str, layer: str, started_at: datetime,
                rows_read: Optional[int] = None, rows_written: Optional[int] = None,
                rows_quarantined: Optional[int] = None, status: str = SUCCESS,
                error_message: Optional[str] = None, ended_at: Optional[datetime] = None) -> tuple:
    """One row in the column order of ops.pipeline_run_log."""
    return (run_id, source_system, step, layer, started_at, ended_at or utc_now(),
            rows_read, rows_written, rows_quarantined, status, error_message)


def log_step(spark: Any, catalog: Optional[str], row: tuple) -> bool:
    """Append one row to <catalog>.ops.pipeline_run_log; False (and nothing written) when there is none."""
    if not catalog:
        return False
    table = f"{catalog}.ops.pipeline_run_log"
    if not spark.catalog.tableExists(table):
        log.warning("%s does not exist; run log row for %s/%s not written", table, row[1], row[2])
        return False
    spark.createDataFrame([row], spark.table(table).schema).write.mode("append").saveAsTable(table)
    return True


def stream_rows(queries: Iterable[Any]) -> Tuple[int, int]:
    """(rows read, rows written) over all micro-batches of finished streaming queries.

    A sink that does not report its output (-1) adds 0 to rows written, so the total is a lower bound
    rather than a guess.
    """
    read = written = 0
    for query in queries:
        for progress in query.recentProgress or []:
            read += int(progress.get("numInputRows") or 0)
            out = (progress.get("sink") or {}).get("numOutputRows", -1)
            written += int(out) if out is not None and int(out) >= 0 else 0
    return read, written
