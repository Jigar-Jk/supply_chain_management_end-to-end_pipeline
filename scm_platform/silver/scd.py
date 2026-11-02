"""
SCD Type 1 / Type 2 dimension maintenance and point-in-time surrogate key lookup.

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from typing import Any, List, Optional

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        coalesce,
        col,
        concat_ws,
        current_timestamp,
        lit,
        md5,
        min as spark_min,
        struct,
        to_json,
    )
    from pyspark.sql.types import TimestampType
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.quality import deduplicate_dataframe


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
