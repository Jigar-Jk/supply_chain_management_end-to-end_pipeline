"""
ingest_api.py – Job entry point for REST sources: pull new records from an API into the landing zone.

Local (watermark in a JSON file, files in a folder):
    $env:CARRIER_API_KEY = "local-dev-key"
    python -m scm_platform.ingest_api --source carrier --base-url http://localhost:8080 --landing data/_api_landing

Databricks (watermark in <catalog>.ops.api_watermark, files written with dbutils to the landing container):
    scm-ingest-api --source carrier --base-url https://<carrier-api> --catalog scm_platform_dev \
        --landing abfss://landing@<account>.dfs.core.windows.net/shipments --run-id {{job.run_id}}

The API key is read from an environment variable (default CARRIER_API_KEY; on Databricks set from a
secret scope), never from an argument: job arguments show up in run pages and logs.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from scm_platform.api.carrier import fetch_shipments
from scm_platform.api.client import ApiClient
from scm_platform.api.landing import DbutilsLanding, LocalLanding
from scm_platform.api.watermark import DEFAULT_STATE_FILE, FileWatermarkStore, TableWatermarkStore

SOURCES = {"carrier": fetch_shipments}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull new records from a REST API into the landing zone.")
    parser.add_argument("--source", required=True, choices=sorted(SOURCES))
    parser.add_argument("--base-url", required=True, help="API root, e.g. http://localhost:8080")
    parser.add_argument("--landing", required=True, help="Folder or abfss:// path the feed's Auto Loader reads")
    parser.add_argument("--catalog", default=None, help="Databricks: keep the watermark in <catalog>.ops.api_watermark")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="Local: watermark JSON file")
    parser.add_argument("--run-id", default=None, help="Job run id (Databricks passes {{job.run_id}})")
    parser.add_argument("--api-key-env", default="CARRIER_API_KEY", help="Environment variable holding the key")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--initial-days", type=int, default=90, help="First run: fetch changes this many days back")
    args, _ = parser.parse_known_args(argv)
    return args


def run(args: argparse.Namespace, session=None, spark=None, dbutils=None):
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise SystemExit(f"Set {args.api_key_env} to the API key (on Databricks: from the secret scope).")
    client = ApiClient(args.base_url, api_key=api_key, session=session)

    if args.catalog:
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        store = TableWatermarkStore(spark, args.catalog)
    else:
        store = FileWatermarkStore(Path(args.state_file))

    if args.landing.startswith(("abfss://", "dbfs:/", "/Volumes/")):
        if dbutils is None:
            from pyspark.dbutils import DBUtils           # only on Databricks
            dbutils = DBUtils(spark)
        landing = DbutilsLanding(args.landing, dbutils)
    else:
        landing = LocalLanding(Path(args.landing))

    run_id = args.run_id or "local-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return SOURCES[args.source](client, store, landing, run_id,
                                page_size=args.page_size, initial_days=args.initial_days)


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    result = run(parse_args(argv))
    print(f"{result.rows} rows in {len(result.files)} files; watermark {result.watermark_before} -> "
          f"{result.watermark_after}")


if __name__ == "__main__":
    main()
