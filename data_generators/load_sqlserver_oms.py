"""
load_sqlserver_oms.py – Load generated orders + inventory into the SQL Server OMS source (scm_oms).

This is the CDC source system's state before Debezium starts: Debezium's initial snapshot reads
these rows (op 'r'), then streams every later change (op 'c' / 'u' / 'd').

Primary keys mean a real OMS table cannot hold a null or duplicate key, so such rows are skipped
and counted. Every other bad value (negative price, shipped > ordered, unknown customer, ...) is
loaded as-is, so the Silver quality gate still has to catch it after CDC.

Connects with Windows authentication (your admin login); the debezium_cdc login is read-only.

Usage:
    python -m data_generators.load_sqlserver_oms            # load into localhost / scm_oms
    python -m data_generators.load_sqlserver_oms --reset    # empty both tables first, then load
"""

import argparse
import json
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
BATCH_SIZE = 5000

ORDER_COLUMNS = [
    "order_id", "order_line_number", "customer_id", "product_id", "warehouse_id", "order_date",
    "promised_delivery_date", "ordered_quantity", "shipped_quantity", "unit_price", "order_status",
    "updated_at",
]
INVENTORY_COLUMNS = [
    "warehouse_id", "product_id", "quantity_on_hand", "quantity_reserved", "safety_stock",
    "reorder_point", "updated_at",
]


def connect(server: str, database: str):
    import pyodbc
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;",
        autocommit=False,
    )


def _date(value):
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def _timestamp(value):
    """'2026-09-12T12:06:49Z' -> naive UTC datetime (SQL Server DATETIME2 has no time zone)."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _decimal(value):
    return Decimal(str(value)) if value is not None else None


def _read_jsonl(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def order_rows(path: Path):
    rows, seen, skipped = [], set(), Counter()
    for r in _read_jsonl(path):
        key = (r.get("order_id"), r.get("order_line_number"))
        if not key[0] or key[1] is None:
            skipped["null_key"] += 1
            continue
        if key in seen:
            skipped["duplicate_key"] += 1
            continue
        seen.add(key)
        rows.append((
            r["order_id"], r["order_line_number"], r.get("customer_id"), r.get("product_id"),
            r.get("warehouse_id"), _date(r.get("order_date")), _date(r.get("promised_delivery_date")),
            r.get("ordered_quantity"), r.get("shipped_quantity"), _decimal(r.get("unit_price")),
            r.get("order_status"), _timestamp(r.get("updated_at")),
        ))
    return rows, skipped


def inventory_rows(path: Path):
    rows, seen, skipped = [], set(), Counter()
    for r in _read_jsonl(path):
        key = (r.get("warehouse_id"), r.get("product_id"))
        if not key[0] or not key[1]:
            skipped["null_key"] += 1
            continue
        if key in seen:
            skipped["duplicate_key"] += 1
            continue
        seen.add(key)
        rows.append((
            r["warehouse_id"], r["product_id"], r.get("quantity_on_hand"), r.get("quantity_reserved"),
            r.get("safety_stock"), r.get("reorder_point"), _timestamp(r.get("recorded_at")),
        ))
    return rows, skipped


def insert(conn, table: str, columns: list, rows: list) -> None:
    sql = f"INSERT INTO dbo.{table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})"
    cursor = conn.cursor()
    cursor.fast_executemany = True
    for start in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[start:start + BATCH_SIZE])
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load generated data into the SQL Server OMS source.")
    parser.add_argument("--server", default="localhost")
    parser.add_argument("--database", default="scm_oms")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--reset", action="store_true",
                        help="DELETE all rows first (TRUNCATE is not allowed on CDC-enabled tables)")
    args = parser.parse_args()

    try:
        orders, order_skips = order_rows(args.data_dir / "orders" / "orders.jsonl")
        snapshots = sorted((args.data_dir / "inventory").glob("inventory_*.jsonl"))
        if not snapshots:
            raise FileNotFoundError("No data/inventory/inventory_*.jsonl. Run generate_inventory first.")
        inventory, inventory_skips = inventory_rows(snapshots[-1])

        conn = connect(args.server, args.database)
        if args.reset:
            conn.cursor().execute("DELETE FROM dbo.order_lines; DELETE FROM dbo.inventory;")
            conn.commit()
        insert(conn, "order_lines", ORDER_COLUMNS, orders)
        insert(conn, "inventory", INVENTORY_COLUMNS, inventory)
        conn.close()

        print(f"order_lines: loaded {len(orders)}, skipped {dict(order_skips)}")
        print(f"inventory  : loaded {len(inventory)} from {snapshots[-1].name}, skipped {dict(inventory_skips)}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
