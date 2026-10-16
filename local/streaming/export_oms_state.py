"""
export_oms_state.py – Dump the SQL Server OMS's current order_lines to CSV for CDC reconciliation.

Runs on Windows (Windows authentication). Writes data/_local_lake/recon/order_lines_source.csv,
which reconcile_cdc.py (inside the Spark container) compares with the state Silver derives from
the CDC change stream.

Usage:
    python -m local.streaming.export_oms_state
"""

import csv
import sys
from pathlib import Path

from data_generators.load_sqlserver_oms import connect

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUT = PROJECT_ROOT / "data" / "_local_lake" / "recon" / "order_lines_source.csv"
COLUMNS = ["order_id", "order_line_number", "customer_id", "product_id", "warehouse_id", "order_status",
           "ordered_quantity", "shipped_quantity", "unit_price"]


def main() -> None:
    try:
        conn = connect("localhost", "scm_oms")
        rows = conn.cursor().execute(f"SELECT {', '.join(COLUMNS)} FROM dbo.order_lines").fetchall()
        conn.close()
        OUT.parent.mkdir(parents=True, exist_ok=True)
        with open(OUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)
            writer.writerows(rows)
        print(f"Exported {len(rows)} order lines from SQL Server -> {OUT}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
