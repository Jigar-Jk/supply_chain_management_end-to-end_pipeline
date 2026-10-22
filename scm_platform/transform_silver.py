"""
transform_silver.py – Silver Layer Galaxy Schema Transformations & Quality Gate.

Implements the Supply Chain Control Tower Silver Layer complying with the project charter:
1. Three Fact Tables:
   - silver.fact_order: Grain is one row per order line (Source: SQL Server OMS).
   - silver.fact_shipment: Grain is one row per shipment leg (Source: Carrier tracking / dispatch).
   - silver.fact_inventory_snapshot: Grain is one row per warehouse, product, snapshot date (Source: WMS).
2. Six Conformed Dimensions:
   - silver.dim_customer (SCD Type 2)
   - silver.dim_product (SCD Type 2)
   - silver.dim_warehouse (SCD Type 2)
   - silver.dim_carrier (SCD Type 1 — guide §8.1)
   - silver.dim_route (SCD Type 1 — guide §8.1)
   - silver.dim_date (Static Type 0 calendar dimension)
3. SCD Type 2 Dimension Maintenance (customer, product, warehouse):
   - Surrogate key generation (hash of natural key + effective_from).
   - Historical preservation: attribute changes close existing current version (effective_to, is_current=False)
     and insert a new current version (effective_from, effective_to=None, is_current=True).
   - Stale-record protection: prevents older versions from overwriting newer active records.
   - Point-in-time and current-version fact-to-dimension surrogate key resolution.
4. Charter-Compliant Centralized Quarantine:
   - Dead-letter schema: quarantine_id, original_payload, failure_reason, failed_rule, batch_id, source_system, ingested_at, status.
   - Status flow support: PENDING -> REPROCESSED or REJECTED.
   - Idempotent quarantine merge on quarantine_id.
5. Deterministic Deduplication:
   - Business freshness ordering -> Ingest timestamp -> Stable canonical payload hash tie-breaking.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import (
        coalesce,
        col,
        concat_ws,
        current_timestamp,
        current_timezone,
        expr,
        from_json,
        lit,
        max as spark_max,
        md5,
        min as spark_min,
        row_number,
        struct,
        to_json,
        to_utc_timestamp,
        when,
    )
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
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


# ---------------------------------------------------------------------------
# 3. SCD Type 1 / Type 2 Dimension Maintenance
# ---------------------------------------------------------------------------

def _is_empty(df: Optional[DataFrame]) -> bool:
    """True when df is None or has no rows. Avoids the RDD API, which UC shared/serverless compute rejects."""
    return df is None or len(df.head(1)) == 0


def apply_scd1_dimension(
    df_incoming: DataFrame,
    df_existing: Optional[DataFrame] = None,
    natural_key: str = "carrier_id",
    effective_from_col: str = "effective_from",
    sk_col: str = "carrier_sk",
) -> DataFrame:
    """Apply SCD Type 1 (overwrite in place) to a dimension.

    - One row per natural key; surrogate key is md5(natural_key) and never changes.
    - Incoming attributes overwrite existing ones; attributes absent from the incoming
      feed keep their existing values.
    - effective_from / created_at record first sight of the key; effective_to is always NULL
      and is_current always TRUE, so fact lookups can treat Type 1 and Type 2 dims uniformly.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for SCD Type 1 processing.")

    deduped_incoming = deduplicate_dataframe(
        df_incoming, key_col=natural_key, business_freshness_col=effective_from_col
    ).withColumn(sk_col, md5(col(natural_key).cast("string")))

    if _is_empty(df_existing):
        return (
            deduped_incoming
            .withColumn(
                "effective_from",
                col(effective_from_col) if effective_from_col in deduped_incoming.columns else current_timestamp(),
            )
            .withColumn("effective_to", lit(None).cast(TimestampType()))
            .withColumn("is_current", lit(True))
            .withColumn("created_at", current_timestamp())
        )

    target_cols = df_existing.columns
    target_types = {f.name: f.dataType for f in df_existing.schema.fields}

    untouched = df_existing.join(deduped_incoming.select(natural_key), natural_key, "left_anti")

    ex = df_existing.select([col(c).alias(f"_ex_{c}") for c in target_cols])
    joined = deduped_incoming.join(ex, col(natural_key) == col(f"_ex_{natural_key}"), "left")

    inc_eff = col(effective_from_col) if effective_from_col in deduped_incoming.columns else current_timestamp()
    projected = []
    for c in target_cols:
        if c == sk_col:
            expr_c = coalesce(col(f"_ex_{c}"), col(sk_col))
        elif c == "effective_from":
            expr_c = coalesce(col("_ex_effective_from"), inc_eff)
        elif c == "effective_to":
            expr_c = lit(None)
        elif c == "is_current":
            expr_c = lit(True)
        elif c == "created_at":
            expr_c = coalesce(col("_ex_created_at"), current_timestamp())
        elif c in deduped_incoming.columns:
            expr_c = col(c)
        else:
            expr_c = col(f"_ex_{c}")
        projected.append(expr_c.cast(target_types[c]).alias(c))

    return untouched.select(*target_cols).unionByName(joined.select(*projected))


def apply_scd2_dimension(
    df_incoming: DataFrame,
    df_existing: Optional[DataFrame] = None,
    natural_key: str = "customer_id",
    tracked_cols: Optional[List[str]] = None,
    effective_from_col: str = "effective_from",
    sk_col: str = "customer_sk",
) -> DataFrame:
    """Apply SCD Type 2 history versioning to a dimension.

    Behavior:
    1. New natural keys: inserted with is_current=True, effective_to=NULL.
    2. Unchanged keys: existing current record is preserved as-is.
    3. Changed keys: existing current version is closed (effective_to = incoming.effective_from, is_current=False),
       and a new version is inserted (effective_from = incoming.effective_from, effective_to=NULL, is_current=True).
    4. Stale record protection: incoming records with effective_from <= existing current effective_from are ignored.
    5. Pre-existing historical versions (is_current=False) are preserved without modification.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for SCD Type 2 processing.")

    tracked_cols = tracked_cols or []

    # Defensive deduplication on incoming records keeping freshest
    deduped_incoming = deduplicate_dataframe(
        df_incoming, key_col=natural_key, business_freshness_col=effective_from_col
    )

    # Initial load when existing dimension is absent or empty
    if _is_empty(df_existing):
        initial_df = deduped_incoming.withColumn(
            "effective_from",
            col(effective_from_col) if effective_from_col in deduped_incoming.columns else current_timestamp(),
        ).withColumn(
            "effective_to",
            lit(None).cast(TimestampType()),
        ).withColumn(
            "is_current",
            lit(True),
        ).withColumn(
            sk_col,
            md5(concat_ws("_", col(natural_key), col("effective_from").cast("string"))),
        ).withColumn(
            "created_at",
            current_timestamp(),
        )
        return initial_df

    # Existing records partitioned into closed (historical) and active (current)
    existing_closed = df_existing.filter(col("is_current") == False)
    existing_current = df_existing.filter(col("is_current") == True)

    target_cols = df_existing.columns
    # Compare only attributes present in both the feed and the dimension, so an optional
    # attribute (e.g. unit_cost on an older feed) neither crashes nor forces a new version.
    tracked_cols = [c for c in tracked_cols if c in deduped_incoming.columns and c in target_cols]

    # Prepare prefixed projections for join
    inc_prefixed = deduped_incoming.select([col(c).alias(f"inc_{c}") for c in deduped_incoming.columns])
    curr_prefixed = existing_current.select([col(c).alias(f"curr_{c}") for c in existing_current.columns])

    joined = inc_prefixed.join(
        curr_prefixed,
        col(f"inc_{natural_key}") == col(f"curr_{natural_key}"),
        "full_outer",
    )

    # Tracked attribute changes comparison. Fields are aliased back to their bare names so the
    # JSON keys match on both sides; otherwise inc_x vs curr_x would always hash differently.
    inc_hash = md5(to_json(struct(*[col(f"inc_{c}").alias(c) for c in tracked_cols])))
    curr_hash = md5(to_json(struct(*[col(f"curr_{c}").alias(c) for c in tracked_cols])))

    is_new_entity = col(f"curr_{natural_key}").isNull() & col(f"inc_{natural_key}").isNotNull()
    is_retained_current = col(f"inc_{natural_key}").isNull() & col(f"curr_{natural_key}").isNotNull()

    # Stale record check: if incoming effective_from <= current effective_from, treat as stale (no overwrite)
    inc_eff = col(f"inc_{effective_from_col}") if f"inc_{effective_from_col}" in joined.columns else current_timestamp()
    curr_eff = col("curr_effective_from")
    is_stale = col(f"curr_{natural_key}").isNotNull() & col(f"inc_{natural_key}").isNotNull() & (inc_eff <= curr_eff)

    has_changed = (
        col(f"curr_{natural_key}").isNotNull()
        & col(f"inc_{natural_key}").isNotNull()
        & (inc_hash != curr_hash)
        & (~is_stale)
    )
    is_unchanged = (
        col(f"curr_{natural_key}").isNotNull()
        & col(f"inc_{natural_key}").isNotNull()
        & ((inc_hash == curr_hash) | is_stale)
    )

    # 1. Unchanged existing current records (either not in incoming, or incoming attributes identical, or incoming was stale)
    unchanged_rows = (
        joined.filter(is_retained_current | is_unchanged)
        .select([col(f"curr_{c}").alias(c) for c in target_cols])
    )

    # 2. Brand new entities
    new_entity_rows = (
        joined.filter(is_new_entity)
        .withColumn("effective_from", inc_eff)
        .withColumn("effective_to", lit(None).cast(TimestampType()))
        .withColumn("is_current", lit(True))
        .withColumn(sk_col, md5(concat_ws("_", col(f"inc_{natural_key}"), col("effective_from").cast("string"))))
        .withColumn("created_at", current_timestamp())
    )
    # Project to target_cols, filling any absent attributes with NULL
    new_entity_select = []
    for c in target_cols:
        if c in ["effective_from", "effective_to", "is_current", sk_col, "created_at"]:
            new_entity_select.append(col(c))
        elif f"inc_{c}" in new_entity_rows.columns:
            new_entity_select.append(col(f"inc_{c}").alias(c))
        else:
            new_entity_select.append(lit(None).alias(c))
    new_entity_projected = new_entity_rows.select(*new_entity_select)

    # 3. Changed entities -> Close old version
    closed_old_rows = (
        joined.filter(has_changed)
        .withColumn("effective_to", inc_eff)
        .withColumn("is_current", lit(False))
    )
    closed_old_select = []
    for c in target_cols:
        if c in ["effective_to", "is_current"]:
            closed_old_select.append(col(c))
        else:
            closed_old_select.append(col(f"curr_{c}").alias(c))
    closed_old_projected = closed_old_rows.select(*closed_old_select)

    # 4. Changed entities -> Insert new current version
    new_version_rows = (
        joined.filter(has_changed)
        .withColumn("effective_from", inc_eff)
        .withColumn("effective_to", lit(None).cast(TimestampType()))
        .withColumn("is_current", lit(True))
        .withColumn(sk_col, md5(concat_ws("_", col(f"inc_{natural_key}"), col("effective_from").cast("string"))))
        .withColumn("created_at", current_timestamp())
    )
    new_version_select = []
    for c in target_cols:
        if c in ["effective_from", "effective_to", "is_current", sk_col, "created_at"]:
            new_version_select.append(col(c))
        elif f"inc_{c}" in new_version_rows.columns:
            new_version_select.append(col(f"inc_{c}").alias(c))
        else:
            new_version_select.append(lit(None).alias(c))
    new_version_projected = new_version_rows.select(*new_version_select)

    # Combine all slices: historical + unchanged active + closed active + new versions + new entities
    result = (
        existing_closed.select(*target_cols)
        .unionByName(unchanged_rows)
        .unionByName(closed_old_projected)
        .unionByName(new_version_projected)
        .unionByName(new_entity_projected)
    )

    return result


def lookup_dimension_sk(
    df_fact: DataFrame,
    df_dim: DataFrame,
    natural_key: str,
    sk_col: str,
    fact_timestamp_col: Optional[str] = None,
) -> DataFrame:
    """Resolve dimension surrogate key for a fact DataFrame.

    Supports:
    - Point-in-time lookup if fact_timestamp_col is provided (historical accuracy).
      The earliest version of each key is treated as open-ended to the past, so facts
      dated before the dimension was first loaded (the normal case for a first load,
      where effective_from is the ingest time) still resolve instead of failing RI.
    - Current active version (is_current=True) lookup if fact_timestamp_col is None.
    """
    first_version = Window.partitionBy(natural_key)
    dim_sub = df_dim.withColumn(
        "_dim_first_eff", spark_min("effective_from").over(first_version)
    ).select(
        col(natural_key).alias(f"_dim_{natural_key}"),
        col(sk_col),
        col("effective_from").alias("_dim_eff_from"),
        col("effective_to").alias("_dim_eff_to"),
        col("is_current").alias("_dim_is_current"),
        col("_dim_first_eff"),
    )

    if fact_timestamp_col and fact_timestamp_col in df_fact.columns:
        join_cond = (
            (col(natural_key) == col(f"_dim_{natural_key}"))
            & (
                (col(fact_timestamp_col) >= col("_dim_eff_from"))
                | (col("_dim_eff_from") == col("_dim_first_eff"))
            )
            & (col("_dim_eff_to").isNull() | (col(fact_timestamp_col) < col("_dim_eff_to")))
        )
    else:
        join_cond = (
            (col(natural_key) == col(f"_dim_{natural_key}"))
            & (col("_dim_is_current") == True)
        )

    joined = df_fact.join(dim_sub, join_cond, "left")
    return joined.drop(f"_dim_{natural_key}", "_dim_eff_from", "_dim_eff_to", "_dim_is_current", "_dim_first_eff")


# ---------------------------------------------------------------------------
# 4. Conformed Dimensions Transformations
# ---------------------------------------------------------------------------

def generate_dim_date(
    spark: SparkSession,
    start_date_str: str = "2020-01-01",
    end_date_str: str = "2027-12-31",
) -> DataFrame:
    """Generate conformed static calendar date dimension (Type 0)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate dim_date.")

    start_d = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_d = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    rows = []
    curr = start_d
    while curr <= end_d:
        date_sk = int(curr.strftime("%Y%m%d"))
        qtr = (curr.month - 1) // 3 + 1
        dow = curr.weekday() + 1  # 1 = Monday, 7 = Sunday
        is_wknd = curr.weekday() in (5, 6)
        rows.append((
            date_sk,
            curr,
            curr.year,
            qtr,
            curr.month,
            curr.strftime("%B"),
            curr.day,
            dow,
            curr.strftime("%A"),
            is_wknd,
            False,  # is_holiday
        ))
        curr += timedelta(days=1)

    schema = StructType([
        StructField("date_sk", IntegerType(), False),
        StructField("calendar_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day_of_month", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("is_holiday", BooleanType(), False),
    ])

    return spark.createDataFrame(rows, schema=schema)


def _with_effective_from(df: DataFrame) -> DataFrame:
    """effective_from = ingest time when known, else now."""
    return df.withColumn(
        "effective_from",
        col("_ingest_timestamp") if "_ingest_timestamp" in df.columns else current_timestamp(),
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


# Rule sets: the single source of truth for each dimension's quality gate (order = priority).
ROUTE_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_ROUTE_ID_MISSING", "route_id"),
    QualityRule(
        "RULE_ROUTE_COORDINATES",
        "origin_longitude IS NULL OR origin_latitude IS NULL OR destination_longitude IS NULL "
        "OR destination_latitude IS NULL OR NOT (origin_longitude BETWEEN -180 AND 180 "
        "AND destination_longitude BETWEEN -180 AND 180 AND origin_latitude BETWEEN -90 AND 90 "
        "AND destination_latitude BETWEEN -90 AND 90)",
        f"concat('Coordinates missing or out of range: origin (', {_fmt('origin_latitude')}, ', ', "
        f"{_fmt('origin_longitude')}, ') destination (', {_fmt('destination_latitude')}, ', ', "
        f"{_fmt('destination_longitude')}, ')')",
    ),
    QualityRule("RULE_ROUTE_DISTANCE_NON_POSITIVE", "distance_meters IS NULL OR distance_meters <= 0",
                f"concat('distance_meters must be > 0, got ', {_fmt('distance_meters')})"),
    QualityRule("RULE_ROUTE_DURATION_NON_POSITIVE", "duration_seconds IS NULL OR duration_seconds <= 0",
                f"concat('duration_seconds must be > 0, got ', {_fmt('duration_seconds')})"),
    QualityRule("RULE_ROUTE_ATTRIBUTES_MISSING", "routing_profile IS NULL OR source IS NULL",
                "'Missing routing_profile or source'"),
]

CARRIER_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_CARRIER_ID_EMPTY", "carrier_id"),
    QualityRule("RULE_CARRIER_OTIF_TARGET_RANGE", "contracted_otif_target NOT BETWEEN 0 AND 100",
                f"concat('contracted_otif_target must be 0-100, got ', {_fmt('contracted_otif_target')})",
                requires=("contracted_otif_target",)),
]

CUSTOMER_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_CUSTOMER_ID_EMPTY", "customer_id"),
]

PRODUCT_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_PRODUCT_ID_MISSING", "product_id"),
    QualityRule("RULE_PRODUCT_WEIGHT_NON_POSITIVE", "unit_weight_kg <= 0",
                f"concat('unit_weight_kg must be > 0, got ', {_fmt('unit_weight_kg')})"),
    QualityRule("RULE_PRODUCT_VOLUME_NON_POSITIVE", "unit_volume_cbm <= 0",
                f"concat('unit_volume_cbm must be > 0, got ', {_fmt('unit_volume_cbm')})"),
    QualityRule("RULE_PRODUCT_COST_NEGATIVE", "unit_cost < 0",
                f"concat('unit_cost must be >= 0, got ', {_fmt('unit_cost')})", requires=("unit_cost",)),
    QualityRule("RULE_PRODUCT_PRICE_NEGATIVE", "unit_price < 0",
                f"concat('unit_price must be >= 0, got ', {_fmt('unit_price')})", requires=("unit_price",)),
]

WAREHOUSE_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_WAREHOUSE_ID_MISSING", "warehouse_id"),
    QualityRule(
        "RULE_WAREHOUSE_COORDINATES",
        "latitude IS NULL OR longitude IS NULL OR NOT (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)",
        f"concat('Coordinates missing or out of range: (', {_fmt('latitude')}, ', ', {_fmt('longitude')}, ')')",
    ),
]


def transform_dim_route(
    df_bronze_routes: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate, clean, deduplicate and apply SCD Type 1 for silver.dim_route.

    Source: OpenRouteService / GIS road network data (bronze.routes).
    Type 1 per implementation guide §8.1: a route is derived from its origin/destination pair,
    so corrections overwrite in place. Quality rules: ROUTE_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_routes, key_col="route_id")
    valid, quar = apply_quality_rules(deduped, ROUTE_RULES, "ORS", batch_id, "route_id")
    quar_routes = quarantine_duplicates(duplicates, "route_id", "ORS", batch_id).unionByName(quar)

    dim_route = apply_scd1_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="route_id",
        effective_from_col="effective_from",
        sk_col="route_sk",
    )
    return dim_route, quar_routes


CARRIER_MASTER_COLUMNS = [
    "carrier_id", "carrier_name", "service_level", "transport_mode",
    "contracted_otif_target", "contact_email", "is_active",
]


def transform_dim_carrier(
    df_bronze_carriers: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 1 for silver.dim_carrier.

    Sources:
    - bronze.carriers (carrier contract master) when the frame has a carrier_name column;
    - otherwise falls back to deriving carriers from bronze.shipments carrier_id values.
    Type 1 per implementation guide §8.1: carrier changes are name/service-level corrections only.
    Quality rules: CARRIER_RULES.
    """
    if "carrier_name" in df_bronze_carriers.columns:
        deduped, duplicates = split_duplicates(df_bronze_carriers, key_col="carrier_id")
        valid, quar = apply_quality_rules(deduped, CARRIER_RULES, "CARRIER_MASTER", batch_id, "carrier_id")
        quar_carriers = quarantine_duplicates(duplicates, "carrier_id", "CARRIER_MASTER", batch_id).unionByName(quar)
        carriers = _with_effective_from(
            valid.select(*[c for c in CARRIER_MASTER_COLUMNS if c in valid.columns],
                         *([col("_ingest_timestamp")] if "_ingest_timestamp" in valid.columns else []))
        )
    else:
        # Fallback: carriers seen on shipments. Repeats of a carrier_id are expected here
        # (one per shipment), so they are collapsed rather than quarantined as duplicates.
        valid, quar_carriers = apply_quality_rules(
            df_bronze_carriers, CARRIER_RULES, "CARRIER_API", batch_id, "carrier_id"
        )
        carriers = (
            valid.select(col("carrier_id"))
            .distinct()
            .withColumn("carrier_name", expr("concat_ws(' ', split(carrier_id, '_')[1], 'Logistics')"))
            .withColumn("service_level", lit("STANDARD"))
            .withColumn("contact_email", expr("concat(lower(carrier_id), '@carrier-network.com')"))
            .withColumn("is_active", lit(True))
            .withColumn("effective_from", current_timestamp())
        )

    dim_carrier = apply_scd1_dimension(
        df_incoming=carriers,
        df_existing=df_existing_dim,
        natural_key="carrier_id",
        effective_from_col="effective_from",
        sk_col="carrier_sk",
    )
    return dim_carrier, quar_carriers


def transform_dim_customer(
    df_bronze_customers: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_customer.

    Source Contract: SQL Server OMS customer entity data (CDC feed).
    Tracked SCD2 attributes: customer_name, customer_segment, country, city.
    Quality rules: CUSTOMER_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_customers, key_col="customer_id")
    valid, quar = apply_quality_rules(deduped, CUSTOMER_RULES, "SQLSERVER_OMS", batch_id, "customer_id")
    quar_customers = quarantine_duplicates(duplicates, "customer_id", "SQLSERVER_OMS", batch_id).unionByName(quar)

    dim_customer = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="customer_id",
        tracked_cols=["customer_name", "customer_segment", "country", "city"],
        effective_from_col="effective_from",
        sk_col="customer_sk",
    )
    return dim_customer, quar_customers


def transform_dim_product(
    df_bronze_products: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_product.

    Source Contract: SQL Server OMS / ERP item master (CDC feed).
    Tracked SCD2 attributes: product_name, category, unit_weight_kg, unit_volume_cbm,
    unit_cost, unit_price (cost/price are optional in the feed).
    Quality rules: PRODUCT_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_products, key_col="product_id")
    valid, quar = apply_quality_rules(deduped, PRODUCT_RULES, "SQLSERVER_OMS", batch_id, "product_id")
    quar_products = quarantine_duplicates(duplicates, "product_id", "SQLSERVER_OMS", batch_id).unionByName(quar)

    dim_product = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="product_id",
        tracked_cols=["product_name", "category", "unit_weight_kg", "unit_volume_cbm", "unit_cost", "unit_price"],
        effective_from_col="effective_from",
        sk_col="product_sk",
    )
    return dim_product, quar_products


def transform_dim_warehouse(
    df_bronze_warehouses: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_warehouse.

    Source Contract: WMS warehouse master (batch extract).
    Tracked SCD2 attributes: warehouse_name, facility_type, city, country, latitude, longitude, capacity_sqm.
    Quality rules: WAREHOUSE_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_warehouses, key_col="warehouse_id")
    valid, quar = apply_quality_rules(deduped, WAREHOUSE_RULES, "WMS", batch_id, "warehouse_id")
    quar_warehouses = quarantine_duplicates(duplicates, "warehouse_id", "WMS", batch_id).unionByName(quar)

    dim_warehouse = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="warehouse_id",
        tracked_cols=["warehouse_name", "facility_type", "city", "country", "latitude", "longitude", "capacity_sqm"],
        effective_from_col="effective_from",
        sk_col="warehouse_sk",
    )
    return dim_warehouse, quar_warehouses


# ---------------------------------------------------------------------------
# 5. Galaxy Schema Fact Tables Transformations
# ---------------------------------------------------------------------------

# Bronze GPS columns that are transport/audit metadata, not part of the event payload
GPS_NON_PAYLOAD_COLUMNS = (
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "event_ts",
    "_source_system", "_rescued_data",
)


def gps_rules() -> List[QualityRule]:
    """Quality-gate rules for GPS/RFID events from bronze.gps_events (order = priority)."""
    return [
        _missing_key_rule("RULE_GPS_EVENT_ID_MISSING", "event_id"),
        _missing_key_rule("RULE_GPS_TRUCK_ID_MISSING", "truck_id"),
        _missing_key_rule("RULE_GPS_SHIPMENT_ID_MISSING", "shipment_id"),
        QualityRule("RULE_GPS_TIMESTAMP_INVALID", "event_ts IS NULL",
                    f"concat('Event timestamp missing or unparseable: ', {_fmt('ts')})"),
        QualityRule("RULE_GPS_COORDINATES",
                    "lat IS NULL OR lon IS NULL OR NOT (lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180)",
                    f"concat('Coordinates missing or out of range: (', {_fmt('lat')}, ', ', {_fmt('lon')}, ')')"),
        QualityRule("RULE_GPS_SPEED_IMPLAUSIBLE", "speed_kmh < 0 OR speed_kmh > 160",
                    f"concat('speed_kmh must be 0-160, got ', {_fmt('speed_kmh')})"),
    ]


def transform_gps_events(
    df_bronze_gps: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate Bronze GPS/RFID events: (clean_events, quarantine).

    Re-delivered events (same event_id) that slipped past the Bronze watermark dedup are
    collapsed here, not quarantined: at-least-once delivery is a transport property, not bad
    data, and every copy remains in append-only Bronze. Unparseable payloads never reach this
    table; they are in bronze.gps_events_dlq. Quality rules: gps_rules().
    """
    deduped, _redeliveries = split_duplicates(
        df_bronze_gps, key_col="event_id", tie_breaker_col="_ingest_timestamp",
    )
    return apply_quality_rules(
        deduped, gps_rules(), "GPS_STREAM", batch_id, "event_id",
        payload_exclude=GPS_NON_PAYLOAD_COLUMNS,
    )


def latest_ping_per_shipment(df_gps_clean: DataFrame) -> DataFrame:
    """Most recent clean ping per shipment: shipment_id, truck_id, last_lat, last_lon, last_ping_ts."""
    window = Window.partitionBy("shipment_id").orderBy(col("event_ts").desc(), col("event_id").desc())
    return (
        df_gps_clean.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .select(
            col("shipment_id").alias("_ping_shipment_id"),
            col("truck_id"),
            col("lat").alias("last_lat"),
            col("lon").alias("last_lon"),
            col("event_ts").alias("last_ping_ts"),
        )
    )


SHIPMENT_WORKING_COLUMNS = (
    "_dispatch_ts", "_planned_ts", "_actual_ts", "_updated_ts", "_key_max_fresh",
    "carrier_sk", "route_sk", "origin_warehouse_sk",
)

SHIPMENT_STATUSES = ("DISPATCHED", "IN_TRANSIT", "DELIVERED", "EXCEPTION")


def shipment_rules(has_warehouse_dim: bool) -> List[QualityRule]:
    """Quality-gate rules for shipments, evaluated after dimension lookups (order = priority)."""
    statuses = ", ".join(f"'{x}'" for x in SHIPMENT_STATUSES)
    rules = [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_SHIPMENT_ID_MISSING", "shipment_id"),
        QualityRule("RULE_TIMESTAMP_PARSE", "_dispatch_ts IS NULL OR _planned_ts IS NULL",
                    f"concat('Unparseable dispatch or planned delivery timestamp: ', {_fmt('dispatch_time_utc')}, "
                    f"' / ', {_fmt('planned_delivery_time_utc')})"),
        QualityRule("RULE_CHRONOLOGICAL_DISPATCH", "_planned_ts <= _dispatch_ts",
                    f"concat('planned_delivery_time ', {_fmt('planned_delivery_time_utc')}, "
                    f"' is not after dispatch_time ', {_fmt('dispatch_time_utc')})"),
        QualityRule("RULE_ACTUAL_BEFORE_DISPATCH", "_actual_ts <= _dispatch_ts",
                    f"concat('actual_delivery_time ', {_fmt('actual_delivery_time_utc')}, "
                    f"' is not after dispatch_time ', {_fmt('dispatch_time_utc')})",
                    requires=("actual_delivery_time_utc",)),
        QualityRule("RULE_SHIPMENT_STATUS_INVALID", f"shipment_status IS NULL OR shipment_status NOT IN ({statuses})",
                    f"concat('shipment_status must be one of {', '.join(SHIPMENT_STATUSES)}, got ', "
                    f"{_fmt('shipment_status')})",
                    requires=("shipment_status",)),
        QualityRule("RULE_DELIVERED_WITHOUT_TIMESTAMP", "shipment_status = 'DELIVERED' AND _actual_ts IS NULL",
                    "'shipment_status is DELIVERED but actual_delivery_time is missing or unparseable'",
                    requires=("shipment_status", "actual_delivery_time_utc")),
        QualityRule("RULE_SHIPPED_EXCEEDS_ORDERED", "shipped_quantity < 0 OR shipped_quantity > ordered_quantity",
                    f"concat('shipped_quantity ', {_fmt('shipped_quantity')}, "
                    f"' outside 0..ordered_quantity ', {_fmt('ordered_quantity')})",
                    requires=("shipped_quantity", "ordered_quantity")),
        QualityRule("RULE_MANDATORY_FIELDS", "data_type IS NULL", "'Missing mandatory field data_type'",
                    requires=("data_type",)),
        QualityRule("RULE_REF_INTEGRITY_CARRIER", "carrier_sk IS NULL",
                    f"concat('Referential integrity failure: carrier_id ', {_fmt('carrier_id')}, "
                    "' does not exist in silver.dim_carrier')"),
        QualityRule("RULE_REF_INTEGRITY_ROUTE", "route_sk IS NULL",
                    f"concat('Referential integrity failure: route_id ', {_fmt('route_id')}, "
                    "' does not exist in silver.dim_route')"),
    ]
    if has_warehouse_dim:
        rules.append(QualityRule(
            "RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_id IS NOT NULL AND origin_warehouse_sk IS NULL",
            f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
            "' not found in silver.dim_warehouse')",
            requires=("warehouse_id",),
        ))
    return rules


def transform_fact_shipment(
    df_bronze_shipments: DataFrame,
    df_dim_carrier: DataFrame,
    df_dim_route: DataFrame,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
    df_dim_warehouse: Optional[DataFrame] = None,
    df_last_ping: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze shipments into silver.fact_shipment.

    df_last_ping (latest_ping_per_shipment over clean GPS events) adds the truck's latest
    position: truck_id, last_lat, last_lon, last_ping_ts (NULL when no ping has been received).

    Grain: one row per shipment leg (single-leg shipments today: leg_sequence = 1).
    Source: carrier shipments generated from shipped/delivered orders (bronze.shipments).

    Versions vs duplicates (split_latest_versions): a shipment is updated as it moves
    (IN_TRANSIT -> DELIVERED); the latest updated_at wins, rows tied with it are RULE_DUPLICATE_KEY.

    KPI columns (delivered shipments; NULL while in transit):
    - delay_minutes = actual_delivery_time - planned_delivery_time (carrier's promised ETA)
    - is_on_time    = actual_delivery_time <= planned_delivery_time
    - is_in_full    = shipped_quantity >= ordered_quantity
    Quality rules: shipment_rules().
    """
    cols = df_bronze_shipments.columns

    def ts(name):
        return expr(f"try_to_timestamp(cast({name} as string))") if name in cols else lit(None).cast(TimestampType())

    prepared = (
        df_bronze_shipments
        .withColumn("_dispatch_ts", ts("dispatch_time_utc"))
        .withColumn("_planned_ts", ts("planned_delivery_time_utc"))
        .withColumn("_actual_ts", ts("actual_delivery_time_utc"))
        .withColumn("_updated_ts", ts("updated_at") if "updated_at" in cols else col("_dispatch_ts"))
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "shipment_id", "_updated_ts", "CARRIER_API", batch_id,
        payload_exclude=SHIPMENT_WORKING_COLUMNS,
    )

    # Dimension keys as of the dispatch time (point-in-time; Type 1 dims have one version)
    with_carrier = lookup_dimension_sk(
        kept, df_dim_carrier, natural_key="carrier_id", sk_col="carrier_sk", fact_timestamp_col="_dispatch_ts"
    )
    with_route = lookup_dimension_sk(
        with_carrier, df_dim_route, natural_key="route_id", sk_col="route_sk", fact_timestamp_col="_dispatch_ts"
    )
    if df_dim_warehouse is not None and "warehouse_id" in cols:
        with_wh = lookup_dimension_sk(
            with_route,
            df_dim_warehouse.withColumnRenamed("warehouse_sk", "origin_warehouse_sk"),
            natural_key="warehouse_id",
            sk_col="origin_warehouse_sk",
            fact_timestamp_col="_dispatch_ts",
        )
    else:
        with_wh = with_route.withColumn("origin_warehouse_sk", lit(None).cast(StringType()))

    valid, quar_rules = apply_quality_rules(
        with_wh, shipment_rules(df_dim_warehouse is not None), "CARRIER_API", batch_id, "shipment_id",
        payload_exclude=SHIPMENT_WORKING_COLUMNS,
    )

    def optional(name, dtype, default=None):
        return col(name).cast(dtype) if name in cols else lit(default).cast(dtype)

    ordered = optional("ordered_quantity", IntegerType())
    shipped = optional("shipped_quantity", IntegerType())
    hours_between = lambda a, b: (col(b).cast("long") - col(a).cast("long")) / 3600.0  # noqa: E731

    fact_df = valid.select(
        md5(concat_ws("_", col("shipment_id"), lit("1"))).alias("shipment_leg_sk"),
        col("shipment_id"),
        lit(1).alias("leg_sequence"),
        col("carrier_sk"),
        col("route_sk"),
        col("origin_warehouse_sk"),
        expr("cast(date_format(_dispatch_ts, 'yyyyMMdd') as int)").alias("dispatch_date_sk"),
        expr("cast(date_format(_planned_ts, 'yyyyMMdd') as int)").alias("planned_delivery_date_sk"),
        col("_dispatch_ts").alias("dispatch_time"),
        col("_planned_ts").alias("planned_delivery_time"),
        hours_between("_dispatch_ts", "_planned_ts").alias("planned_transit_hours"),
        col("_actual_ts").alias("actual_delivery_time"),
        when(col("_actual_ts").isNotNull(), hours_between("_dispatch_ts", "_actual_ts")).alias("actual_transit_hours"),
        optional("shipment_status", StringType(), "DISPATCHED").alias("shipment_status"),
        col("carrier_id"),
        col("route_id"),
        optional("data_type", StringType()).alias("data_type"),
        optional("order_id", StringType()).alias("order_id"),
        optional("warehouse_id", StringType()).alias("warehouse_id"),
        ordered.alias("ordered_quantity"),
        shipped.alias("shipped_quantity"),
        when(col("_actual_ts").isNotNull(),
             ((col("_actual_ts").cast("long") - col("_planned_ts").cast("long")) / 60).cast(IntegerType())
             ).alias("delay_minutes"),
        when(col("_actual_ts").isNotNull(), col("_actual_ts") <= col("_planned_ts")).alias("is_on_time"),
        when(ordered.isNotNull() & shipped.isNotNull(), shipped >= ordered).alias("is_in_full"),
        optional("weather_risk", StringType()).alias("weather_risk"),
        col("_updated_ts").alias("updated_at"),
        current_timestamp().alias("created_at"),
    )

    ping_columns = ["truck_id", "last_lat", "last_lon", "last_ping_ts"]
    if df_last_ping is not None:
        fact_df = (
            fact_df.join(df_last_ping, fact_df["shipment_id"] == df_last_ping["_ping_shipment_id"], "left")
            .drop("_ping_shipment_id")
        )
    else:
        fact_df = fact_df.select(
            "*",
            lit(None).cast(StringType()).alias("truck_id"),
            lit(None).cast(DoubleType()).alias("last_lat"),
            lit(None).cast(DoubleType()).alias("last_lon"),
            lit(None).cast(TimestampType()).alias("last_ping_ts"),
        )
    ordered_columns = [c for c in fact_df.columns if c not in ping_columns + ["created_at"]]
    fact_df = fact_df.select(*ordered_columns, *ping_columns, "created_at")

    return fact_df, quar_duplicates.unionByName(quar_rules)


# ---------------------------------------------------------------------------
# CDC: decode Debezium change events from Bronze into source rows
# ---------------------------------------------------------------------------

def _image_schema(fields) -> Any:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# Row images as Debezium encodes them: DATE = days since 1970-01-01, DATETIME2 = epoch millis,
# DECIMAL = double (connector setting decimal.handling.mode=double).
ORDER_LINE_IMAGE_FIELDS = [
    ("order_id", StringType()), ("order_line_number", IntegerType()), ("customer_id", StringType()),
    ("product_id", StringType()), ("warehouse_id", StringType()), ("order_date", IntegerType()),
    ("promised_delivery_date", IntegerType()), ("ordered_quantity", IntegerType()),
    ("shipped_quantity", IntegerType()), ("unit_price", DoubleType()), ("order_status", StringType()),
    ("updated_at", LongType()),
]
INVENTORY_IMAGE_FIELDS = [
    ("warehouse_id", StringType()), ("product_id", StringType()), ("quantity_on_hand", IntegerType()),
    ("quantity_reserved", IntegerType()), ("safety_stock", IntegerType()), ("reorder_point", IntegerType()),
    ("updated_at", LongType()),
]
CDC_COLUMNS = ("_op", "_is_deleted", "_source_position")


def decode_cdc_rows(
    df_cdc: DataFrame,
    image_fields,
    date_columns: Sequence[str] = (),
    timestamp_columns: Sequence[str] = (),
) -> DataFrame:
    """One row per Bronze CDC event: the source row as typed columns, plus _op, _is_deleted, _source_position.

    The row comes from the `after` image, or from `before` for deletes (a delete has no after),
    so a deleted row keeps its last known values and can be soft-deleted downstream.
    Dates are decoded from days-since-epoch to 'yyyy-MM-dd' strings and timestamps from epoch
    millis to TIMESTAMP, matching what the file feeds deliver.
    """
    image = when(col("op") == "d", col("before")).otherwise(col("after"))
    audit = ["_ingest_timestamp"] if "_ingest_timestamp" in df_cdc.columns else []
    decoded = df_cdc.select(
        from_json(image, _image_schema(image_fields)).alias("_img"),
        col("op").alias("_op"),
        (col("op") == "d").alias("_is_deleted"),
        col("source_position").alias("_source_position"),
        *audit,
    ).select("_img.*", *CDC_COLUMNS, *audit)
    for c in date_columns:
        decoded = decoded.withColumn(c, expr(f"cast(date_add(DATE'1970-01-01', {c}) AS STRING)"))
    for c in timestamp_columns:
        decoded = decoded.withColumn(c, expr(f"timestamp_millis({c})"))
    return decoded


def decode_order_line_cdc(df_orders_cdc: DataFrame) -> DataFrame:
    """bronze.orders_cdc -> order-line rows shaped like the bronze.orders file feed (+ CDC columns)."""
    return decode_cdc_rows(df_orders_cdc, ORDER_LINE_IMAGE_FIELDS,
                           date_columns=("order_date", "promised_delivery_date"), timestamp_columns=("updated_at",))


def decode_inventory_cdc(df_inventory_cdc: DataFrame) -> DataFrame:
    """bronze.inventory_cdc -> current stock rows (+ CDC columns)."""
    return decode_cdc_rows(df_inventory_cdc, INVENTORY_IMAGE_FIELDS, timestamp_columns=("updated_at",))


def drop_cdc_redeliveries(df: DataFrame, key_col: str) -> DataFrame:
    """Collapse events re-sent by Debezium (same key AND same source position).

    At-least-once delivery is a transport property, not bad data: the copies are identical,
    every copy stays in append-only Bronze, and none is quarantined.
    """
    return df.dropDuplicates([key_col, "_source_position"])


def latest_cdc_rows(decoded: DataFrame, key_cols: Sequence[str]) -> DataFrame:
    """Latest decoded CDC row per key (highest _source_position), deleted rows included.

    The pure "apply" step without dimensions or quality rules: what the source table looks like
    according to the change stream. Used to reconcile Silver against the source database.
    """
    key = concat_ws("_", *[col(c).cast("string") for c in key_cols])
    window = Window.partitionBy(key).orderBy(col("_source_position").desc())
    return (drop_cdc_redeliveries(decoded.withColumn("_key", key), "_key")
            .withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1).drop("_rn", "_key"))


ORDER_STATUSES = ("CREATED", "PICKED", "SHIPPED", "DELIVERED", "CANCELLED")

ORDER_WORKING_COLUMNS = (
    "_grain_key", "_key_max_fresh", "_order_ts", "_updated_ts",
    "customer_sk", "product_sk", "origin_warehouse_sk", *CDC_COLUMNS,
)


def order_rules(has_warehouse_dim: bool) -> List[QualityRule]:
    """Quality-gate rules for order lines, evaluated after dimension lookups (order = priority)."""
    rules = [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_ORDER_ID_MISSING", "order_id"),
        QualityRule("RULE_ORDER_LINE_NUMBER_INVALID", "order_line_number IS NULL OR order_line_number <= 0",
                    f"concat('order_line_number must be > 0, got ', {_fmt('order_line_number')})"),
        QualityRule("RULE_ORDER_DATE_INVALID", "_order_ts IS NULL",
                    f"concat('order_date missing or unparseable: ', {_fmt('order_date')})"),
        QualityRule("RULE_ORDER_DATE_IN_FUTURE", "_order_ts > current_timestamp()",
                    f"concat('order_date is in the future: ', {_fmt('order_date')})"),
        QualityRule("RULE_ORDER_QUANTITY_POSITIVE", "ordered_quantity IS NULL OR ordered_quantity <= 0",
                    f"concat('ordered_quantity must be > 0, got ', {_fmt('ordered_quantity')})"),
        QualityRule("RULE_UNIT_PRICE_NON_NEGATIVE", "unit_price IS NULL OR unit_price < 0",
                    f"concat('unit_price must be >= 0, got ', {_fmt('unit_price')})"),
        QualityRule("RULE_PROMISED_BEFORE_ORDER",
                    "try_to_timestamp(cast(promised_delivery_date as string)) < _order_ts",
                    f"concat('promised_delivery_date ', {_fmt('promised_delivery_date')}, "
                    f"' is before order_date ', {_fmt('order_date')})",
                    requires=("promised_delivery_date",)),
        QualityRule("RULE_SHIPPED_EXCEEDS_ORDERED",
                    "shipped_quantity < 0 OR shipped_quantity > ordered_quantity",
                    f"concat('shipped_quantity ', {_fmt('shipped_quantity')}, "
                    f"' outside 0..ordered_quantity ', {_fmt('ordered_quantity')})",
                    requires=("shipped_quantity",)),
        QualityRule("RULE_ORDER_STATUS_INVALID",
                    f"order_status IS NULL OR order_status NOT IN ({', '.join(repr(x) for x in ORDER_STATUSES)})",
                    f"concat('order_status must be one of {', '.join(ORDER_STATUSES)}, got ', {_fmt('order_status')})",
                    requires=("order_status",)),
        QualityRule("RULE_REF_INTEGRITY_CUSTOMER", "customer_sk IS NULL",
                    f"concat('Referential integrity failure: customer_id ', {_fmt('customer_id')}, "
                    "' not found in silver.dim_customer')"),
        QualityRule("RULE_REF_INTEGRITY_PRODUCT", "product_sk IS NULL",
                    f"concat('Referential integrity failure: product_id ', {_fmt('product_id')}, "
                    "' not found in silver.dim_product')"),
    ]
    if has_warehouse_dim:
        rules.append(QualityRule(
            "RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_id IS NOT NULL AND origin_warehouse_sk IS NULL",
            f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
            "' not found in silver.dim_warehouse')",
            requires=("warehouse_id",),
        ))
    return rules


def transform_fact_order(
    df_bronze_orders: DataFrame,
    df_dim_customer: DataFrame,
    df_dim_product: DataFrame,
    df_dim_warehouse: Optional[DataFrame] = None,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze order lines into silver.fact_order.

    Grain: one row per order line (order_id + order_line_number).
    Sources: the OMS order-line file feed (bronze.orders), or Debezium CDC events decoded with
    decode_order_line_cdc (bronze.orders_cdc; SQL Server in place of the charter's Oracle).

    Versions vs duplicates (split_latest_versions): the latest version wins; older versions are
    superseded history, rows tied with the winner are RULE_DUPLICATE_KEY.
    - File feed: "latest" = updated_at.
    - CDC: "latest" = source_position (log order), so a stale event replayed late can never win;
      events re-sent by Debezium (same position) are collapsed first (drop_cdc_redeliveries), and a
      delete as the latest event yields the row's last values with is_deleted = TRUE (soft delete).

    Dimension keys resolve point-in-time as of the order date. Quality rules: order_rules().
    """
    cols = df_bronze_orders.columns
    is_cdc = "_source_position" in cols
    fresh_col = "_source_position" if is_cdc else ("_updated_ts" if "updated_at" in cols else "_order_ts")
    has_key = (
        col("order_id").isNotNull() & (col("order_id") != "") & col("order_line_number").isNotNull()
    )
    prepared = (
        df_bronze_orders
        .withColumn("_order_ts", expr("try_to_timestamp(cast(order_date as string))"))
        .withColumn(
            "_updated_ts",
            expr("try_to_timestamp(cast(updated_at as string))") if "updated_at" in cols else col("_order_ts"),
        )
        .withColumn(
            "_grain_key",
            when(has_key, concat_ws("_", col("order_id"), col("order_line_number").cast("string"))),
        )
    )
    if is_cdc:
        prepared = drop_cdc_redeliveries(prepared, "_grain_key")
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", fresh_col, "SQLSERVER_OMS", batch_id,
        key_label="order line", payload_exclude=ORDER_WORKING_COLUMNS,
    )

    # Resolve dimension surrogate keys as of the order date (SCD2 point-in-time)
    with_cust = lookup_dimension_sk(
        kept, df_dim_customer, natural_key="customer_id", sk_col="customer_sk", fact_timestamp_col="_order_ts"
    )
    with_prod = lookup_dimension_sk(
        with_cust, df_dim_product, natural_key="product_id", sk_col="product_sk", fact_timestamp_col="_order_ts"
    )
    if df_dim_warehouse is not None and "warehouse_id" in cols:
        with_wh = lookup_dimension_sk(
            with_prod,
            df_dim_warehouse.withColumnRenamed("warehouse_sk", "origin_warehouse_sk"),
            natural_key="warehouse_id",
            sk_col="origin_warehouse_sk",
            fact_timestamp_col="_order_ts",
        )
    else:
        with_wh = with_prod.withColumn("origin_warehouse_sk", lit(None).cast(StringType()))

    valid, quar_rules = apply_quality_rules(
        with_wh, order_rules(df_dim_warehouse is not None), "SQLSERVER_OMS", batch_id, "order_id",
        payload_exclude=ORDER_WORKING_COLUMNS,
    )

    def optional(name, dtype, default=None):
        return col(name).cast(dtype) if name in cols else lit(default).cast(dtype)

    fact_df = valid.select(
        md5(concat_ws("_", col("order_id"), col("order_line_number").cast("string"))).alias("order_line_sk"),
        col("order_id"),
        col("order_line_number").cast(IntegerType()).alias("order_line_number"),
        col("customer_sk"),
        col("product_sk"),
        col("origin_warehouse_sk"),
        expr("cast(date_format(_order_ts, 'yyyyMMdd') as int)").alias("order_date_sk"),
        (expr("cast(date_format(try_to_timestamp(cast(promised_delivery_date as string)), 'yyyyMMdd') as int)")
         if "promised_delivery_date" in cols else lit(None).cast(IntegerType())).alias("promised_delivery_date_sk"),
        col("ordered_quantity").cast(IntegerType()).alias("ordered_quantity"),
        optional("shipped_quantity", IntegerType()).alias("shipped_quantity"),
        col("unit_price").cast(DoubleType()).alias("unit_price"),
        (col("ordered_quantity") * col("unit_price")).cast(DoubleType()).alias("total_price"),
        optional("order_status", StringType(), "CREATED").alias("order_status"),
        col("customer_id"),
        col("product_id"),
        optional("warehouse_id", StringType()).alias("warehouse_id"),
        col("_updated_ts").alias("updated_at"),
        optional("_is_deleted", BooleanType(), False).alias("is_deleted"),
        optional("_source_position", StringType()).alias("source_position"),
        current_timestamp().alias("created_at"),
    )

    return fact_df, quar_duplicates.unionByName(quar_rules)


# ---------------------------------------------------------------------------
# Real-time stock position from inventory CDC
# ---------------------------------------------------------------------------

INVENTORY_CURRENT_WORKING_COLUMNS = ("_grain_key", "_key_max_fresh", *CDC_COLUMNS)


def inventory_current_rules() -> List[QualityRule]:
    """Quality-gate rules for live stock rows from inventory CDC (order = priority)."""
    return [
        _missing_key_rule("RULE_INVENTORY_WAREHOUSE_ID_MISSING", "warehouse_id"),
        _missing_key_rule("RULE_INVENTORY_PRODUCT_ID_MISSING", "product_id"),
        QualityRule("RULE_QTY_ON_HAND_NON_NEGATIVE", "quantity_on_hand IS NULL OR quantity_on_hand < 0",
                    f"concat('quantity_on_hand must be >= 0, got ', {_fmt('quantity_on_hand')})"),
        QualityRule("RULE_QTY_RESERVED_INVALID", "quantity_reserved < 0 OR quantity_reserved > quantity_on_hand",
                    f"concat('quantity_reserved ', {_fmt('quantity_reserved')}, "
                    f"' outside 0..quantity_on_hand ', {_fmt('quantity_on_hand')})"),
    ]


def transform_inventory_current(
    df_inventory_cdc_rows: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Latest stock position per warehouse x product from decoded inventory CDC (decode_inventory_cdc).

    Complements the nightly WMS reconciliation (fact_inventory_snapshot, the system of record):
    this is the intraday view the OMS maintains as orders reserve and ship stock.
    Same CDC semantics as fact_order: highest source_position wins, redeliveries collapse,
    a delete soft-deletes. Quality rules: inventory_current_rules().
    """
    has_key = (col("warehouse_id").isNotNull() & (col("warehouse_id") != "")
               & col("product_id").isNotNull() & (col("product_id") != ""))
    prepared = drop_cdc_redeliveries(
        df_inventory_cdc_rows.withColumn(
            "_grain_key", when(has_key, concat_ws("_", col("warehouse_id"), col("product_id")))),
        "_grain_key",
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", "_source_position", "SQLSERVER_OMS", batch_id,
        key_label="warehouse/product", payload_exclude=INVENTORY_CURRENT_WORKING_COLUMNS,
    )
    valid, quar_rules = apply_quality_rules(
        kept, inventory_current_rules(), "SQLSERVER_OMS", batch_id, "warehouse_id",
        payload_exclude=INVENTORY_CURRENT_WORKING_COLUMNS,
    )
    current = valid.select(
        md5(concat_ws("_", col("warehouse_id"), col("product_id"))).alias("inventory_current_sk"),
        col("warehouse_id"),
        col("product_id"),
        col("quantity_on_hand").cast(IntegerType()).alias("quantity_on_hand"),
        col("quantity_reserved").cast(IntegerType()).alias("quantity_reserved"),
        (col("quantity_on_hand") - coalesce(col("quantity_reserved"), lit(0))).cast(IntegerType())
        .alias("quantity_available"),
        col("safety_stock").cast(IntegerType()).alias("safety_stock"),
        col("reorder_point").cast(IntegerType()).alias("reorder_point"),
        col("updated_at"),
        col("_is_deleted").alias("is_deleted"),
        col("_source_position").alias("source_position"),
        current_timestamp().alias("created_at"),
    )
    return current, quar_duplicates.unionByName(quar_rules)


INVENTORY_WORKING_COLUMNS = (
    "_grain_key", "_snapshot_ts", "_recorded_ts", "warehouse_sk", "product_sk",
)


def inventory_rules() -> List[QualityRule]:
    """Quality-gate rules for WMS snapshot rows, evaluated after dimension lookups (order = priority)."""
    return [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_INVENTORY_WAREHOUSE_ID_MISSING", "warehouse_id"),
        _missing_key_rule("RULE_INVENTORY_PRODUCT_ID_MISSING", "product_id"),
        QualityRule("RULE_SNAPSHOT_DATE_INVALID", "_snapshot_ts IS NULL",
                    f"concat('snapshot_date missing or unparseable: ', {_fmt('snapshot_date')})"),
        QualityRule("RULE_SNAPSHOT_DATE_IN_FUTURE", "_snapshot_ts > current_timestamp()",
                    f"concat('snapshot_date is in the future: ', {_fmt('snapshot_date')})"),
        QualityRule("RULE_QTY_ON_HAND_NON_NEGATIVE", "quantity_on_hand IS NULL OR quantity_on_hand < 0",
                    f"concat('quantity_on_hand must be >= 0, got ', {_fmt('quantity_on_hand')})"),
        QualityRule("RULE_QTY_RESERVED_INVALID",
                    "quantity_reserved < 0 OR quantity_reserved > quantity_on_hand",
                    f"concat('quantity_reserved ', {_fmt('quantity_reserved')}, "
                    f"' outside 0..quantity_on_hand ', {_fmt('quantity_on_hand')})",
                    requires=("quantity_reserved",)),
        QualityRule("RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_sk IS NULL",
                    f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
                    "' not found in silver.dim_warehouse')"),
        QualityRule("RULE_REF_INTEGRITY_PRODUCT", "product_sk IS NULL",
                    f"concat('Referential integrity failure: product_id ', {_fmt('product_id')}, "
                    "' not found in silver.dim_product')"),
    ]


def transform_fact_inventory_snapshot(
    df_bronze_inventory: DataFrame,
    df_dim_warehouse: DataFrame,
    df_dim_product: DataFrame,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze WMS reconciliation rows into silver.fact_inventory_snapshot.

    Grain: one row per warehouse, product, and snapshot date.
    Source Contract: WMS nightly stock reconciliation NDJSON file.

    Versions vs duplicates (split_latest_versions): when the same snapshot arrives twice, the
    later recorded_at wins (a re-sent corrected file); rows tied with it are RULE_DUPLICATE_KEY.

    Measures: quantity_available = on_hand - reserved; is_stockout = available <= 0 (stockout
    rate KPI); is_stockout_risk = available <= safety_stock. Quality rules: inventory_rules().
    """
    cols = df_bronze_inventory.columns
    fresh_col = "_recorded_ts" if "recorded_at" in cols else "_snapshot_ts"
    has_key = (
        col("warehouse_id").isNotNull() & (col("warehouse_id") != "")
        & col("product_id").isNotNull() & (col("product_id") != "")
        & col("snapshot_date").isNotNull()
    )
    prepared = (
        df_bronze_inventory
        .withColumn("_snapshot_ts", expr("try_to_timestamp(cast(snapshot_date as string))"))
        .withColumn(
            "_recorded_ts",
            expr("try_to_timestamp(cast(recorded_at as string))") if "recorded_at" in cols else col("_snapshot_ts"),
        )
        .withColumn(
            "_grain_key",
            when(has_key, concat_ws("_", col("warehouse_id"), col("product_id"), col("snapshot_date").cast("string"))),
        )
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", fresh_col, "WMS", batch_id,
        key_label="warehouse/product/snapshot_date", payload_exclude=INVENTORY_WORKING_COLUMNS,
    )

    # Resolve warehouse & product surrogate keys as of the snapshot date (SCD2 point-in-time)
    with_wh = lookup_dimension_sk(
        kept, df_dim_warehouse, natural_key="warehouse_id", sk_col="warehouse_sk", fact_timestamp_col="_snapshot_ts"
    )
    with_prod = lookup_dimension_sk(
        with_wh, df_dim_product, natural_key="product_id", sk_col="product_sk", fact_timestamp_col="_snapshot_ts"
    )

    valid, quar_rules = apply_quality_rules(
        with_prod, inventory_rules(), "WMS", batch_id, "warehouse_id",
        payload_exclude=INVENTORY_WORKING_COLUMNS,
    )

    def optional(name, default=None):
        return col(name).cast(DoubleType()) if name in cols else lit(default).cast(DoubleType())

    on_hand = col("quantity_on_hand").cast(DoubleType())
    reserved = coalesce(optional("quantity_reserved"), lit(0.0))
    available = on_hand - reserved
    safety = coalesce(optional("safety_stock"), lit(0.0))
    snapshot_date_sk = expr("cast(date_format(_snapshot_ts, 'yyyyMMdd') as int)")

    fact_df = valid.select(
        md5(concat_ws("_", col("warehouse_id"), col("product_id"), snapshot_date_sk.cast("string")))
        .alias("inventory_snapshot_sk"),
        col("warehouse_sk"),
        col("product_sk"),
        snapshot_date_sk.alias("snapshot_date_sk"),
        expr("to_date(_snapshot_ts)").alias("snapshot_date"),
        on_hand.alias("quantity_on_hand"),
        reserved.alias("quantity_reserved"),
        available.alias("quantity_available"),
        optional("reorder_point").alias("reorder_point"),
        safety.alias("safety_stock"),
        (available <= 0).alias("is_stockout"),
        (available <= safety).alias("is_stockout_risk"),
        col("warehouse_id"),
        col("product_id"),
        col("_recorded_ts").alias("recorded_at"),
        lit(False).alias("is_stale"),       # TRUE only for rows carried forward after an SLA miss (sla_checks)
        current_timestamp().alias("created_at"),
    )

    return fact_df, quar_duplicates.unionByName(quar_rules)


# ---------------------------------------------------------------------------
# 6. Auxiliary & Legacy Compatibility Transformations
# ---------------------------------------------------------------------------

def transform_weather(
    df_bronze: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate, clean, deduplicate and split weather into (silver_df, quarantine_df)."""
    deduped = deduplicate_dataframe(
        df_bronze, key_col="weather_record_id", business_freshness_col="forecast_time_utc"
    )
    with_ts = deduped.withColumn("parsed_forecast_time", expr("try_to_timestamp(forecast_time_utc)"))

    valid_cond = (
        col("weather_record_id").isNotNull() & (col("weather_record_id") != "") &
        col("latitude").isNotNull() & (col("latitude") >= -90.0) & (col("latitude") <= 90.0) &
        col("longitude").isNotNull() & (col("longitude") >= -180.0) & (col("longitude") <= 180.0) &
        col("parsed_forecast_time").isNotNull() &
        (col("precipitation_mm").isNull() | (col("precipitation_mm") >= 0)) &
        (col("wind_speed_10m_kmh").isNull() | (col("wind_speed_10m_kmh") >= 0)) &
        (col("visibility_m").isNull() | (col("visibility_m") >= 0)) &
        col("source").isNotNull()
    )

    silver_df = (
        with_ts.filter(valid_cond)
        .select(
            col("weather_record_id"),
            col("latitude"),
            col("longitude"),
            col("parsed_forecast_time").alias("forecast_time"),
            col("temperature_2m_c"),
            col("precipitation_mm"),
            col("wind_speed_10m_kmh"),
            col("visibility_m"),
            col("source"),
            current_timestamp().alias("created_at"),
        )
    )

    invalid_raw = with_ts.filter(~valid_cond)
    clean_cols = [c for c in df_bronze.columns if c not in ["_ingest_timestamp", "_source_file", "_rn"]]
    raw_payload_expr = payload_json(clean_cols)
    quarantine_df = invalid_raw.select(
        md5(concat_ws("_", lit("OPEN_METEO"), coalesce(col("weather_record_id"), lit("none")), raw_payload_expr)).alias("quarantine_id"),
        raw_payload_expr.alias("original_payload"),
        lit("Validation failure: invalid coordinates, unparseable timestamp, or negative weather metrics").alias("failure_reason"),
        lit("RULE_WEATHER_METRICS").alias("failed_rule"),
        lit(batch_id).alias("batch_id"),
        lit("OPEN_METEO").alias("source_system"),
        current_timestamp().alias("ingested_at"),
        lit("PENDING").alias("status"),
        col("weather_record_id").alias("source_record_id"),
        lit("weather").alias("source_table"),
    )

    return silver_df, quarantine_df


def transform_routes(
    df_bronze: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Legacy compatibility adapter: cleans routes and returns clean silver and quarantine DataFrames."""
    dim_routes, quar_routes = transform_dim_route(df_bronze, batch_id=batch_id)
    # Output cleaned silver routes format
    silver_df = dim_routes.filter(col("is_current") == True).select(
        col("route_id"),
        col("origin_longitude"),
        col("origin_latitude"),
        col("destination_longitude"),
        col("destination_latitude"),
        col("distance_meters"),
        col("duration_seconds"),
        col("routing_profile"),
        col("source"),
        col("created_at"),
    )
    # Add legacy compatibility columns to quarantine
    quar_compat = quar_routes.withColumn(
        "source_record_id",
        expr("get_json_object(original_payload, '$.route_id')"),
    ).withColumn(
        "source_table",
        lit("routes"),
    )
    return silver_df, quar_compat


def transform_shipments(
    df_bronze: DataFrame,
    df_valid_routes: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Legacy compatibility adapter: validates shipments against valid routes and returns silver and quarantine."""
    # Build minimal route dim from df_valid_routes
    routes_dim = df_valid_routes.select("route_id").distinct().withColumn(
        "route_sk", md5(col("route_id"))
    ).withColumn("effective_from", current_timestamp()).withColumn(
        "effective_to", lit(None).cast(TimestampType())
    ).withColumn("is_current", lit(True))

    # Build minimal carrier dim from distinct carriers in df_bronze
    carrier_dim = df_bronze.select("carrier_id").filter(col("carrier_id").isNotNull()).distinct().withColumn(
        "carrier_sk", md5(col("carrier_id"))
    ).withColumn("effective_from", current_timestamp()).withColumn(
        "effective_to", lit(None).cast(TimestampType())
    ).withColumn("is_current", lit(True))

    fact_df, quar_df = transform_fact_shipment(
        df_bronze, carrier_dim, routes_dim, batch_id=batch_id
    )

    # Legacy silver projection
    silver_df = fact_df.select(
        col("shipment_id"),
        col("route_id"),
        col("carrier_id"),
        col("dispatch_time"),
        col("planned_delivery_time"),
        col("planned_transit_hours"),
        col("data_type"),
        col("created_at"),
    )

    quar_compat = quar_df.withColumn(
        "source_record_id",
        expr("get_json_object(original_payload, '$.shipment_id')"),
    ).withColumn(
        "source_table",
        lit("shipments"),
    )

    return silver_df, quar_compat


# ---------------------------------------------------------------------------
# 7. Idempotent Target Writers (Delta Lake)
# ---------------------------------------------------------------------------

def _align_to_table(df: DataFrame, spark: SparkSession, table_name: str) -> DataFrame:
    """Project df onto the target table's columns and types.

    Extra working columns (e.g. _ingest_timestamp, legacy source_table) are dropped and
    columns the source lacks are filled with typed NULLs, so MERGE ... INSERT */UPDATE *
    never fails on a schema mismatch.
    """
    target_schema = spark.table(table_name).schema
    return df.select(*[
        (col(f.name) if f.name in df.columns else lit(None)).cast(f.dataType).alias(f.name)
        for f in target_schema.fields
    ])


def merge_update_condition(freshness_col: str) -> str:
    """MERGE WHEN MATCHED condition: never replace a row with an older version.

    A target row without a freshness value (e.g. loaded before CDC) may be updated; otherwise the
    incoming value must be at least as new. For CDC, freshness_col = source_position (log order).
    """
    return f"target.{freshness_col} IS NULL OR source.{freshness_col} >= target.{freshness_col}"


def write_silver_table(
    df: DataFrame,
    table_name: str,
    key_col: str,
    spark: Optional[SparkSession] = None,
    freshness_col: Optional[str] = None,
) -> None:
    """Write silver records into Delta table idempotently using Delta MERGE.

    Guarantees:
    1. Defensive deduplication on key_col before MERGE to prevent multiple-match errors.
    2. Prevention of stale records overwriting newer records via freshness_col comparison.
    3. MERGE errors propagate; there is no silent append fallback that could duplicate rows.
    """
    if spark is None:
        raise ValueError("spark is required to write Silver tables.")

    deduped_source = deduplicate_dataframe(df, key_col=key_col)

    if not spark.catalog.tableExists(table_name):
        # First load when the table was not pre-created by Liquibase.
        deduped_source.write.format("delta").mode("append").saveAsTable(table_name)
        return

    from delta.tables import DeltaTable

    source = _align_to_table(deduped_source, spark, table_name)
    merge_builder = DeltaTable.forName(spark, table_name).alias("target").merge(
        source.alias("source"), f"target.{key_col} = source.{key_col}"
    )
    if freshness_col and freshness_col in source.columns:
        merge_builder = merge_builder.whenMatchedUpdateAll(
            condition=merge_update_condition(freshness_col)
        )
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()


def write_quarantine_table(
    df: DataFrame,
    table_name: str = "quarantine.records",
    spark: Optional[SparkSession] = None,
    source_feed: Optional[str] = None,
) -> None:
    """Idempotently write failed records to quarantine table using Delta MERGE on quarantine_id.

    source_feed tags the rows (and initialises the lifecycle columns) so they can be reprocessed.
    """
    if spark is None:
        raise ValueError("spark is required to write quarantine records.")
    if source_feed is not None:
        df = tag_quarantine_feed(df, source_feed)

    deduped_quarantine = deduplicate_dataframe(df, key_col="quarantine_id")

    if not spark.catalog.tableExists(table_name):
        deduped_quarantine.write.format("delta").mode("append").saveAsTable(table_name)
        return

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, table_name).alias("target")
        .merge(
            _align_to_table(deduped_quarantine, spark, table_name).alias("source"),
            "target.quarantine_id = source.quarantine_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# 8. Pipeline Orchestrator Entrypoint
# ---------------------------------------------------------------------------

def _table_or_none(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Return the table as a DataFrame, or None when it does not exist yet."""
    return spark.table(table_name) if spark.catalog.tableExists(table_name) else None


def _rows_or_none(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Like _table_or_none, but an existing empty table also counts as absent.

    Migration 011 always creates the CDC Bronze tables; until the CDC stream has delivered its first
    event they are empty, and the file feed must stay the source. CDC Bronze is append-only, so once
    it has rows it stays the source.
    """
    df = _table_or_none(spark, table_name)
    return df if df is not None and df.head(1) else None


def run_silver_pipeline(
    spark: SparkSession,
    catalog: str = "scm_platform_dev",
    batch_id: str = "batch_manual",
) -> None:
    """Execute complete Bronze-to-Silver Galaxy Schema transformation pipeline.

    Dimensions are maintained first so facts resolve against this run's versions.
    fact_order and fact_inventory_snapshot run when their Bronze feeds (bronze.orders,
    bronze.inventory) and the dimensions they reference exist.
    """
    quarantine_table = f"{catalog}.quarantine.records"
    run_log_table = f"{catalog}.ops.pipeline_run_log"

    def file_feed(df, feed):
        """Apply agreed renames (COLUMN_ALIASES), then log this run's files rejected for schema drift."""
        if df is None:
            return None
        df = conform_to_contract(df, feed)
        current = df.filter(col("_batch_id") == batch_id) if "_batch_id" in df.columns else df
        rejected = rejected_file_log_rows(schema_drift_report(current), feed, batch_id)
        if spark.catalog.tableExists(run_log_table) and rejected.head(1):
            rejected.write.mode("append").saveAsTable(run_log_table)
        return df

    def load_dimension(name, transform, bronze_df, sk_col, feed):
        existing = _table_or_none(spark, f"{catalog}.silver.{name}")
        dim_df, quar_df = transform(bronze_df, existing, batch_id=batch_id)
        write_silver_table(dim_df, f"{catalog}.silver.{name}", key_col=sk_col, spark=spark)
        write_quarantine_table(quar_df, quarantine_table, spark=spark, source_feed=feed)
        return dim_df

    # 1. Read Bronze tables
    bronze_routes = file_feed(spark.table(f"{catalog}.bronze.routes"), "routes")
    bronze_shipments = file_feed(spark.table(f"{catalog}.bronze.shipments"), "shipments")
    bronze_weather = file_feed(spark.table(f"{catalog}.bronze.weather"), "weather")
    # Carrier master is preferred; fall back to carrier IDs seen on shipments.
    bronze_carriers = file_feed(_table_or_none(spark, f"{catalog}.bronze.carriers"), "carriers")
    if bronze_carriers is None:
        bronze_carriers = bronze_shipments

    # 2. Maintain Conformed Dimensions
    dim_route = load_dimension("dim_route", transform_dim_route, bronze_routes, "route_sk", "routes")
    dim_carrier = load_dimension("dim_carrier", transform_dim_carrier, bronze_carriers, "carrier_sk", "carriers")
    dims = {}
    for name, transform, bronze_name, sk_col in [
        ("dim_warehouse", transform_dim_warehouse, "warehouses", "warehouse_sk"),
        ("dim_product", transform_dim_product, "products", "product_sk"),
        ("dim_customer", transform_dim_customer, "customers", "customer_sk"),
    ]:
        bronze_df = file_feed(_table_or_none(spark, f"{catalog}.bronze.{bronze_name}"), bronze_name)
        if bronze_df is not None:
            dims[name] = load_dimension(name, transform, bronze_df, sk_col, bronze_name)

    dim_date = generate_dim_date(spark)
    write_silver_table(dim_date, f"{catalog}.silver.dim_date", key_col="date_sk", spark=spark)

    # 3. Transform and write Fact Order (needs customer + product dims; warehouse optional)
    # CDC (SQL Server via Debezium) is the order source when present; the file feed otherwise.
    bronze_orders_cdc = _rows_or_none(spark, f"{catalog}.bronze.orders_cdc")
    bronze_orders = (decode_order_line_cdc(bronze_orders_cdc) if bronze_orders_cdc is not None
                     else file_feed(_table_or_none(spark, f"{catalog}.bronze.orders"), "orders"))
    order_freshness = "source_position" if bronze_orders_cdc is not None else "updated_at"
    if bronze_orders is not None and "dim_customer" in dims and "dim_product" in dims:
        fact_order, quar_order = transform_fact_order(
            bronze_orders, dims["dim_customer"], dims["dim_product"], dims.get("dim_warehouse"), dim_date,
            batch_id=batch_id,
        )
        write_silver_table(
            fact_order, f"{catalog}.silver.fact_order", key_col="order_line_sk", spark=spark,
            freshness_col=order_freshness,
        )
        write_quarantine_table(quar_order, quarantine_table, spark=spark, source_feed="orders")

    bronze_inventory_cdc = _rows_or_none(spark, f"{catalog}.bronze.inventory_cdc")
    if bronze_inventory_cdc is not None:
        inventory_current, quar_inventory_current = transform_inventory_current(
            decode_inventory_cdc(bronze_inventory_cdc), batch_id=batch_id)
        write_silver_table(
            inventory_current, f"{catalog}.silver.inventory_current", key_col="inventory_current_sk",
            spark=spark, freshness_col="source_position",
        )
        write_quarantine_table(quar_inventory_current, quarantine_table, spark=spark, source_feed="inventory_cdc")

    # 4. Transform and write Fact Inventory Snapshot (WMS nightly reconciliation)
    bronze_inventory = file_feed(_table_or_none(spark, f"{catalog}.bronze.inventory"), "inventory")
    if bronze_inventory is not None and "dim_warehouse" in dims and "dim_product" in dims:
        fact_inventory, quar_inventory = transform_fact_inventory_snapshot(
            bronze_inventory, dims["dim_warehouse"], dims["dim_product"], dim_date, batch_id=batch_id
        )
        write_silver_table(
            fact_inventory, f"{catalog}.silver.fact_inventory_snapshot", key_col="inventory_snapshot_sk",
            spark=spark, freshness_col="recorded_at",
        )
        write_quarantine_table(quar_inventory, quarantine_table, spark=spark, source_feed="inventory")

    # 5. GPS/RFID events (streamed into bronze.gps_events) -> quarantine + latest ping per shipment
    last_ping = None
    bronze_gps = _table_or_none(spark, f"{catalog}.bronze.gps_events")
    if bronze_gps is not None:
        gps_clean, quar_gps = transform_gps_events(bronze_gps, batch_id=batch_id)
        write_quarantine_table(quar_gps, quarantine_table, spark=spark, source_feed="gps_events")
        last_ping = latest_ping_per_shipment(gps_clean)

    # 6. Transform and write Fact Shipment
    fact_shipment, quar_shipment = transform_fact_shipment(
        bronze_shipments, dim_carrier, dim_route, dim_date, batch_id=batch_id,
        df_dim_warehouse=dims.get("dim_warehouse"), df_last_ping=last_ping,
    )
    write_silver_table(
        fact_shipment, f"{catalog}.silver.fact_shipment", key_col="shipment_leg_sk", spark=spark, freshness_col="updated_at"
    )
    write_quarantine_table(quar_shipment, quarantine_table, spark=spark, source_feed="shipments")

    # 7. Transform weather observations (auxiliary mart)
    silver_weather, quar_weather = transform_weather(bronze_weather, batch_id=batch_id)
    write_silver_table(
        silver_weather, f"{catalog}.silver.weather", key_col="weather_record_id", spark=spark, freshness_col="forecast_time"
    )
    write_quarantine_table(quar_weather, quarantine_table, spark=spark, source_feed="weather")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Run Silver layer Galaxy Schema transformations.")
    parser.add_argument("--catalog", type=str, default="scm_platform_dev", help="Unity Catalog catalog name")
    parser.add_argument("--batch-id", type=str, default="batch_manual", help="Unique batch / job run identifier")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    run_silver_pipeline(spark, catalog=args.catalog, batch_id=args.batch_id)


if __name__ == "__main__":
    main()
