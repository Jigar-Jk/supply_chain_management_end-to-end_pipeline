"""
Quality gate: deduplication, the quarantine record, QualityRule and its application, schema drift.

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        coalesce,
        col,
        concat_ws,
        current_timestamp,
        current_timezone,
        expr,
        lit,
        max as spark_max,
        md5,
        row_number,
        struct,
        to_json,
        to_utc_timestamp,
        when,
    )
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore


# ---------------------------------------------------------------------------
# 1. Deduplication & Freshness Helpers
# ---------------------------------------------------------------------------

def deduplicate_dataframe(
    df: DataFrame,
    key_col: str,
    tie_breaker_col: str = "_ingest_timestamp",
    business_freshness_col: Optional[str] = None,
) -> DataFrame:
    """Deterministically deduplicate a DataFrame on key_col keeping the freshest record.

    Tie-breaking hierarchy:
    1. business_freshness_col DESC (if present and specified)
    2. tie_breaker_col DESC (e.g. _ingest_timestamp)
    3. Canonical payload hash DESC (md5 of struct(*)) to break ties deterministically
       when two records have identical keys and timestamps but conflicting field values.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for DataFrame deduplication.")

    order_cols = []
    if business_freshness_col and business_freshness_col in df.columns:
        order_cols.append(col(business_freshness_col).desc())

    if tie_breaker_col in df.columns:
        order_cols.append(col(tie_breaker_col).desc())

    # Deterministic final tie-breaker: hash of all columns
    payload_hash = expr("md5(to_json(struct(*)))")
    order_cols.append(payload_hash.desc())

    window_spec = Window.partitionBy(key_col).orderBy(*order_cols)
    return df.withColumn("_rn", row_number().over(window_spec)).filter(col("_rn") == 1).drop("_rn")


def split_duplicates(
    df: DataFrame,
    key_col: str,
    tie_breaker_col: str = "_ingest_timestamp",
    business_freshness_col: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Split df into (kept, duplicates) using the same ordering as deduplicate_dataframe.

    Only rows with a non-null, non-empty key compete; rows without a key are passed through in
    `kept` so the null-key validation rule (not the duplicate rule) quarantines them. The
    `duplicates` frame holds every losing row so it can be quarantined instead of silently dropped.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for DataFrame deduplication.")

    has_key = coalesce(col(key_col).isNotNull() & (col(key_col).cast("string") != ""), lit(False))
    keyed = df.filter(has_key)
    unkeyed = df.filter(~has_key)

    order_cols = []
    if business_freshness_col and business_freshness_col in df.columns:
        order_cols.append(col(business_freshness_col).desc())
    if tie_breaker_col in df.columns:
        order_cols.append(col(tie_breaker_col).desc())
    order_cols.append(expr("md5(to_json(struct(*)))").desc())

    ranked = keyed.withColumn("_rn", row_number().over(Window.partitionBy(key_col).orderBy(*order_cols)))
    kept = ranked.filter(col("_rn") == 1).drop("_rn").unionByName(unkeyed)
    duplicates = ranked.filter(col("_rn") > 1).drop("_rn")
    return kept, duplicates


# ---------------------------------------------------------------------------
# 2. Charter-Compliant Quarantine Repository
# ---------------------------------------------------------------------------

VALID_QUARANTINE_STATUSES = {"PENDING", "REPROCESSED", "REJECTED"}


def utc_lit(dt: datetime):
    """A TIMESTAMP literal meaning `dt` in UTC, whatever the Spark session time zone.

    F.lit(datetime) would be interpreted in the driver's LOCAL zone, which silently shifts times
    on any non-UTC machine (e.g. +05:30 on a laptop in India).
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return to_utc_timestamp(lit(dt.strftime("%Y-%m-%d %H:%M:%S.%f")).cast("timestamp"), current_timezone())

# Lifecycle columns added to the charter's 8 quarantine columns (migration 013). source_feed says
# which feed / transform a row belongs to, so reprocess_quarantine can replay it.
QUARANTINE_LIFECYCLE_COLUMNS = [
    ("source_feed", StringType()),
    ("corrected_payload", StringType()),      # steward's fix; original_payload is never edited
    ("retry_count", IntegerType()),
    ("last_retry_at", TimestampType()),
    ("resolved_at", TimestampType()),
    ("resolved_by", StringType()),
    ("resolution_note", StringType()),
]


def tag_quarantine_feed(quarantine: DataFrame, feed: str) -> DataFrame:
    """Tag freshly quarantined rows with their feed and empty lifecycle columns (retry_count 0)."""
    out = quarantine
    for name, dtype in QUARANTINE_LIFECYCLE_COLUMNS:
        value = lit(feed) if name == "source_feed" else (lit(0) if name == "retry_count" else lit(None))
        out = out.withColumn(name, value.cast(dtype))
    return out


def payload_json(columns: List[str]):
    """JSON of the given columns with nulls written explicitly (e.g. "warehouse_id": null).

    Spark's to_json omits null fields by default, which would hide exactly the field that
    failed a null-key rule; the quarantine payload must be the record verbatim.
    """
    return to_json(struct(*[col(c) for c in columns]), {"ignoreNullFields": "false"})


def build_quarantine_record(
    df_invalid: DataFrame,
    failure_reason_expr: str,
    failed_rule_code: str,
    source_system: str,
    batch_id: str,
    record_id_col: Optional[str] = None,
    failed_rule_expr: Optional[str] = None,
    payload_exclude: Sequence[str] = (),
) -> DataFrame:
    """Format invalid records into charter-compliant quarantine schema.

    failed_rule_code is a constant rule code; pass failed_rule_expr (a SQL expression) instead
    when the rule differs per row, as produced by apply_quality_rules.

    Schema:
    - quarantine_id: STRING (deterministic hash)
    - original_payload: STRING (full JSON of source record)
    - failure_reason: STRING (human-readable explanation)
    - failed_rule: STRING (standardized rule code e.g. RULE_REF_INTEGRITY_ROUTE)
    - batch_id: STRING (batch or job run identifier)
    - source_system: STRING (ORS, OPEN_METEO, CARRIER_API, SQLSERVER_OMS, WMS)
    - ingested_at: TIMESTAMP (quarantine arrival timestamp)
    - status: STRING (default 'PENDING')
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build quarantine records.")

    excluded = {"_ingest_timestamp", "_source_file", "_rn", *payload_exclude}
    clean_cols = [c for c in df_invalid.columns if c not in excluded]
    raw_payload_expr = payload_json(clean_cols)

    rec_id_expr = col(record_id_col) if (record_id_col and record_id_col in df_invalid.columns) else lit("none")
    rule_expr = expr(failed_rule_expr) if failed_rule_expr else lit(failed_rule_code)
    quarantine_id_expr = md5(
        concat_ws("_", lit(source_system), rec_id_expr, rule_expr, raw_payload_expr)
    )

    return df_invalid.select(
        quarantine_id_expr.alias("quarantine_id"),
        raw_payload_expr.alias("original_payload"),
        expr(failure_reason_expr).alias("failure_reason"),
        rule_expr.alias("failed_rule"),
        lit(batch_id).alias("batch_id"),
        lit(source_system).alias("source_system"),
        current_timestamp().alias("ingested_at"),
        lit("PENDING").alias("status"),
    )


@dataclass(frozen=True)
class QualityRule:
    """One quality-gate rule.

    violation: SQL predicate that is TRUE when a row breaks the rule. A NULL result counts as
               "not violated", so a rule that treats NULL as invalid must test IS NULL itself.
    reason:    SQL expression for the human-readable failure_reason (quote literal strings).
    requires:  optional columns the rule needs; the rule is skipped when any is absent.
    """
    code: str
    violation: str
    reason: str
    requires: Tuple[str, ...] = ()


def apply_quality_rules(
    df: DataFrame,
    rules: Sequence[QualityRule],
    source_system: str,
    batch_id: str,
    record_id_col: Optional[str] = None,
    payload_exclude: Sequence[str] = (),
) -> Tuple[DataFrame, DataFrame]:
    """Split df into (valid, quarantine) with one ordered rule list as the single source of truth.

    A row is valid only if it breaks no rule. Each quarantined row records the FIRST rule it
    broke (list order = priority), which drives the root-cause breakdown on the DQ dashboard.
    """
    active = [r for r in rules if all(c in df.columns for c in r.requires)]
    if not active:
        return df, build_quarantine_record(
            df.limit(0), "'none'", "RULE_NONE", source_system, batch_id, record_id_col,
            payload_exclude=payload_exclude,
        )

    broken = [f"coalesce(({r.violation}), false)" for r in active]
    any_broken = expr(" OR ".join(broken))
    rule_case = "CASE " + " ".join(f"WHEN {b} THEN '{r.code}'" for r, b in zip(active, broken)) + " END"
    reason_case = "CASE " + " ".join(f"WHEN {b} THEN {r.reason}" for r, b in zip(active, broken)) + " END"

    quarantine = build_quarantine_record(
        df_invalid=df.filter(any_broken),
        failure_reason_expr=reason_case,
        failed_rule_code=None,
        source_system=source_system,
        batch_id=batch_id,
        record_id_col=record_id_col,
        failed_rule_expr=rule_case,
        payload_exclude=payload_exclude,
    )
    return df.filter(~any_broken), quarantine


def quarantine_duplicates(
    df_duplicates: DataFrame,
    key_col: str,
    source_system: str,
    batch_id: str,
    key_label: Optional[str] = None,
    payload_exclude: Sequence[str] = (),
) -> DataFrame:
    """Quarantine the losing rows from split_duplicates under RULE_DUPLICATE_KEY."""
    label = key_label or key_col
    return build_quarantine_record(
        df_invalid=df_duplicates,
        failure_reason_expr=f"concat('Duplicate natural key: a fresher record for {label}=', {key_col}, ' was kept')",
        failed_rule_code="RULE_DUPLICATE_KEY",
        source_system=source_system,
        batch_id=batch_id,
        record_id_col=key_col,
        payload_exclude=payload_exclude,
    )


def split_latest_versions(
    df: DataFrame,
    key_col: str,
    fresh_col: str,
    source_system: str,
    batch_id: str,
    key_label: Optional[str] = None,
    payload_exclude: Sequence[str] = (),
) -> Tuple[DataFrame, DataFrame]:
    """Keep the newest version per key; quarantine only true duplicates.

    When a key appears several times, the row with the latest fresh_col wins. Older rows are
    superseded versions (a CDC update, a re-sent corrected file): they stay in append-only
    Bronze as history and are not quarantined. Rows tied with the winner on fresh_col are
    true duplicates and go to RULE_DUPLICATE_KEY. Returns (kept, duplicate_quarantine).
    """
    with_max = df.withColumn("_key_max_fresh", spark_max(fresh_col).over(Window.partitionBy(key_col)))
    kept, losers = split_duplicates(with_max, key_col=key_col, business_freshness_col=fresh_col)
    true_duplicates = losers.filter(col(fresh_col).eqNullSafe(col("_key_max_fresh")))
    quarantine = quarantine_duplicates(
        true_duplicates, key_col, source_system, batch_id,
        key_label=key_label, payload_exclude=(*payload_exclude, "_key_max_fresh"),
    )
    return kept.drop("_key_max_fresh"), quarantine


def update_quarantine_status(
    df_quarantine: DataFrame,
    quarantine_ids: List[str],
    new_status: str,
) -> DataFrame:
    """Transition quarantine records: PENDING -> REPROCESSED or REJECTED."""
    if new_status not in VALID_QUARANTINE_STATUSES or new_status == "PENDING":
        raise ValueError(
            f"Invalid target status '{new_status}'. Allowed transitions: REPROCESSED, REJECTED"
        )
    if not quarantine_ids:
        return df_quarantine
    return df_quarantine.withColumn(
        "status",
        when(col("quarantine_id").isin(quarantine_ids), lit(new_status)).otherwise(col("status")),
    )


def _missing_key_rule(code: str, key: str) -> QualityRule:
    return QualityRule(code, f"{key} IS NULL OR {key} = ''", f"'Missing or empty {key}'")


def _fmt(column: str) -> str:
    """SQL expression rendering a column value for a failure_reason, NULL-safe."""
    return f"coalesce(cast({column} as string), 'null')"


# ---------------------------------------------------------------------------
# Schema drift: the file no longer matches its feed's contract (ingest_bronze.DATASET_SCHEMAS)
# ---------------------------------------------------------------------------
# Auto Loader (schemaEvolutionMode=rescue) keeps every field that does not fit the contract - a renamed
# or new column, or a value of the wrong type - in _rescued_data as JSON, plus the file path under
# "_file_path". Nothing is lost in Bronze; Silver must not drop it silently, so the drift rule runs
# FIRST in every file feed's rule list: the quarantine then names the real cause ("unexpected column
# qty_on_hand") instead of a symptom ("quantity_on_hand missing").
RESCUED_FIELDS = ("filter(map_keys(from_json(_rescued_data, 'map<string,string>')), "
                  "k -> k NOT IN ('_file_path'))")

SCHEMA_DRIFT_RULE = QualityRule(
    "RULE_SCHEMA_DRIFT",
    f"size({RESCUED_FIELDS}) > 0",
    f"concat('Fields not matching the feed contract (renamed, new or wrong type): ', array_join({RESCUED_FIELDS}, ', '))",
    requires=("_rescued_data",),
)

# Agreed renames, {feed: {name in the file: name in the contract}}. Empty on purpose: the pipeline never
# guesses a mapping (a renamed column may also have changed meaning or units). When a rename is agreed
# with the source team, it is added here through a pull request and conform_to_contract applies it.
COLUMN_ALIASES: Dict[str, Dict[str, str]] = {}

# A file is rejected as a whole when at least this share of its rows drifted (a format change, not a
# few bad rows); scm_daily_pipeline logs it to ops.pipeline_run_log as REJECTED_FILE.
DRIFT_REJECT_SHARE = 0.5

# Which source system each file feed comes from (ops.pipeline_run_log / ops.sla_config naming)
FEED_SOURCE_SYSTEM = {
    "inventory": "wms_batch", "warehouses": "wms_batch",
    "shipments": "carrier_api", "carriers": "carrier_api", "routes": "carrier_api", "weather": "carrier_api",
    "orders": "sqlserver_cdc", "customers": "sqlserver_cdc", "products": "sqlserver_cdc",
}


def conform_to_contract(df: DataFrame, feed: str) -> DataFrame:
    """Apply the agreed renames of COLUMN_ALIASES[feed]: fill the contract column from the rescued field
    and remove that field from _rescued_data, so it no longer counts as drift. No-op without aliases."""
    aliases = COLUMN_ALIASES.get(feed, {})
    if not aliases or "_rescued_data" not in df.columns:
        return df
    out = df
    for source, target in aliases.items():
        rescued = expr(f"get_json_object(_rescued_data, '$.{source}')")
        out = out.withColumn(target, coalesce(col(target), rescued.cast(out.schema[target].dataType)))
    mapped = ", ".join(f"'{s}'" for s in aliases)
    return out.withColumn(
        "_rescued_data",
        expr(f"to_json(map_filter(from_json(_rescued_data, 'map<string,string>'), (k, v) -> k NOT IN ({mapped})))"),
    )


def schema_drift_report(df_bronze: DataFrame, reject_share: float = DRIFT_REJECT_SHARE) -> DataFrame:
    """One row per source file with drifted rows: rows read / drifted, the unexpected fields, and whether
    the whole file is rejected (drifted share >= reject_share). Empty when nothing drifted."""
    schema = "source_file string, rows_read bigint, rows_drifted bigint, unexpected_fields array<string>, rejected boolean"
    if "_rescued_data" not in df_bronze.columns or "_source_file" not in df_bronze.columns:
        return df_bronze.sparkSession.createDataFrame([], schema)
    per_row = df_bronze.select(col("_source_file").alias("source_file"), expr(RESCUED_FIELDS).alias("_fields"))
    return (
        per_row.groupBy("source_file").agg(
            expr("count(*)").alias("rows_read"),
            expr("count_if(coalesce(size(_fields), 0) > 0)").alias("rows_drifted"),
            expr("array_sort(array_distinct(flatten(collect_list(_fields))))").alias("unexpected_fields"),
        )
        .filter(col("rows_drifted") > 0)
        .withColumn("rejected", col("rows_drifted") >= col("rows_read") * lit(reject_share))
    )


def rejected_file_log_rows(report: DataFrame, feed: str, run_id: str) -> DataFrame:
    """ops.pipeline_run_log rows (status REJECTED_FILE) for the files schema_drift_report rejects."""
    now = current_timestamp()
    return report.filter(col("rejected")).select(
        lit(run_id).alias("run_id"),
        lit(FEED_SOURCE_SYSTEM.get(feed, feed)).alias("source_system"),
        expr(f"concat('schema_check:{feed}:', source_file)").alias("step"),
        lit("silver").alias("layer"),
        now.alias("started_at"),
        now.alias("ended_at"),
        col("rows_read").cast(LongType()),
        lit(0).cast(LongType()).alias("rows_written"),
        col("rows_drifted").cast(LongType()).alias("rows_quarantined"),
        lit("REJECTED_FILE").alias("status"),
        expr("concat('Schema drift - fields not matching the feed contract: ', array_join(unexpected_fields, ', '))")
        .alias("error_message"),
    )
