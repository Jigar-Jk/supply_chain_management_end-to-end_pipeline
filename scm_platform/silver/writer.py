"""
Idempotent Delta writers: MERGE with a freshness guard, quarantine writer.

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from typing import Any, Optional

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, lit
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.quality import deduplicate_dataframe, tag_quarantine_feed


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
