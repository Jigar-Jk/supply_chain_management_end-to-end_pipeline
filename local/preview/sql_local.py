"""
sql_local.py – Run deployed Unity Catalog SQL (Gold views, DQ tiles) on local Spark.

Mechanical rewrites only, so what is tested locally is what is deployed:
- CREATE OR REPLACE [MATERIALIZED] VIEW -> CREATE OR REPLACE TEMP VIEW
- silver.x / gold.x                    -> x            (register Silver / Gold as temp views by name)
- bronze.x / quarantine.x / ops.x      -> bronze__x, quarantine__x, ops__x
  (prefixed, because bronze.weather and silver.weather would otherwise collide)
- is_member('group')                    -> true / false  (Databricks-only group check; locally
  nobody is a member unless a test says so, so masking shows *** - the safe default)
"""

import re

PREFIXED_SCHEMAS = ("bronze", "quarantine", "ops")


def localize_sql(sql: str, member_of: tuple = ()) -> str:
    """member_of: the workspace groups the (simulated) current user belongs to."""
    sql = re.sub(r"is_member\('([^']*)'\)", lambda m: "true" if m.group(1) in member_of else "false", sql)
    sql = re.sub(r"CREATE OR REPLACE (MATERIALIZED )?VIEW", "CREATE OR REPLACE TEMP VIEW", sql)
    sql = re.sub(r"\b(silver|gold)\.", "", sql)
    return re.sub(rf"\b({'|'.join(PREFIXED_SCHEMAS)})\.", r"\1__", sql)


def local_name(schema: str, table: str) -> str:
    """Temp view name a table must be registered under for localize_sql'd queries."""
    return f"{schema}__{table}" if schema in PREFIXED_SCHEMAS else table
