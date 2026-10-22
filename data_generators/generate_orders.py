"""
generate_orders.py – Generate synthetic SQL Server OMS order lines for silver.fact_order.

Each record is one order line (the fact_order grain), linked to the master data written by
generate_master_data.py:
    order_id, order_line_number, customer_id, product_id, warehouse_id, order_date,
    promised_delivery_date, ordered_quantity, shipped_quantity, unit_price, order_status,
    updated_at

Only master records that are expected to pass the Silver quality gate are referenced:
entities listed as damaged in data/_manifests/master_data.json are excluded, so every
order-line failure comes from this generator's own, explicitly logged, injection.

Status follows order age relative to the reference time (CREATED -> PICKED -> SHIPPED ->
DELIVERED, with some CANCELLED); delivered lines are occasionally short-shipped so the
fill-rate KPI is non-trivial.

The same order lines also feed SQL Server (load_sqlserver_oms.py) for the CDC path; as a file
feed, updated_at is the business freshness timestamp a CDC feed would carry.

Bad-data injection: ~--bad-rate of lines are corrupted (round-robin over failure modes);
the manifest is written to data/_manifests/orders.json.

Usage:
    python -m data_generators.generate_orders                 # dev scale
    python -m data_generators.generate_orders --scale full    # ~5M order lines
    python -m data_generators.generate_orders --orders 5000 --seed 7
"""

import argparse
import copy
import hashlib
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_RANDOM_SEED = 42
DEFAULT_BAD_RATE = 0.02
DEFAULT_REFERENCE_DATETIME = datetime(2026, 10, 12, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_WINDOW_DAYS = 90

# Orders, not lines: lines per order average ~3 (1-5), so "full" ~= the guide's 5M rows.
SCALE_PRESETS = {"dev": 20_000, "full": 1_700_000}

# Products each warehouse stocks. Orders and the WMS inventory feed must use the same size
# and seed so that every product sold from a warehouse is one it actually holds.
ASSORTMENT_PRESETS = {"dev": 500, "full": 15_000}
DEFAULT_ASSORTMENT_SIZE = ASSORTMENT_PRESETS["dev"]

MAX_LINES_PER_ORDER = 5
LINE_QUANTITY_CHOICES = [1, 1, 2, 3, 5, 10, 12, 20, 24, 50, 100]
CANCEL_RATE = 0.03
SHORT_SHIP_RATE = 0.03   # per line; ~3 lines per order -> ~9% of orders not in full

# Unknown keys used for referential-integrity failures; never generated as master data.
UNKNOWN_CUSTOMER_ID = "CUST-999999"
UNKNOWN_PRODUCT_ID = "SKU-999999"
UNKNOWN_WAREHOUSE_ID = "WH-9999"

FAILURE_MODES = [
    "null_order_id",
    "non_positive_quantity",
    "negative_price",
    "promised_before_order",
    "shipped_exceeds_ordered",
    "future_order_date",
    "unknown_customer",
    "unknown_product",
    "unknown_warehouse",
    "duplicate",
]


# ---------------------------------------------------------------------------
# Master data loading
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run `python -m data_generators.generate_master_data` first.")
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def usable_master_keys(feeds: dict[str, list[dict]], manifest: list[dict]) -> dict[str, list]:
    """Keys of master records expected to land in their dimension.

    Drops null/empty keys and any key the master manifest lists as damaged (other than as a
    duplicate, whose surviving copy is valid). Returns sorted, de-duplicated key lists plus
    a product_id -> unit_price map.
    """
    damaged = {(m["feed"], m["original_key"]) for m in manifest if m["failure_mode"] != "duplicate"}

    def keys(feed: str, key: str) -> list[str]:
        return sorted({r[key] for r in feeds[feed] if r.get(key) and (feed, r[key]) not in damaged})

    customers = keys("customers", "customer_id")
    products = keys("products", "product_id")
    warehouses = keys("warehouses", "warehouse_id")
    if not (customers and products and warehouses):
        raise ValueError("Master data has no usable customers, products or warehouses.")
    usable_products = set(products)
    price = {r["product_id"]: r["unit_price"] for r in feeds["products"] if r.get("product_id") in usable_products}
    return {"customers": customers, "products": products, "warehouses": warehouses, "price": price}


def warehouse_assortment(keys: dict, warehouse_id: str, size: int, seed: int = DEFAULT_RANDOM_SEED) -> list[str]:
    """Deterministic, sorted list of the products a warehouse stocks.

    Shared by generate_orders (what can be sold from a warehouse) and generate_inventory
    (what the WMS reconciles), so sales and stock describe the same assortment.
    """
    rng = random.Random(f"{seed}:assortment:{warehouse_id}")
    return sorted(rng.sample(keys["products"], min(size, len(keys["products"]))))


def expected_daily_units_per_sku(order_count: int, warehouses: int, assortment_size: int,
                                 window_days: int = DEFAULT_WINDOW_DAYS) -> float:
    """Average units ordered per day for one warehouse x product, implied by the order volume.

    generate_inventory sizes its stock simulation from this, so stock levels match what the
    orders feed actually sells (otherwise inventory turnover is meaningless).
    """
    mean_lines = (1 + min(MAX_LINES_PER_ORDER, assortment_size)) / 2
    mean_qty = sum(LINE_QUANTITY_CHOICES) / len(LINE_QUANTITY_CHOICES)
    return order_count / window_days * mean_lines * mean_qty / (warehouses * assortment_size)


def load_master_data(data_dir: Path) -> tuple[dict[str, list[dict]], list[dict]]:
    feeds = {
        "customers": _read_jsonl(data_dir / "customers" / "customers.jsonl"),
        "products": _read_jsonl(data_dir / "products" / "products.jsonl"),
        "warehouses": _read_jsonl(data_dir / "warehouses" / "warehouses.jsonl"),
    }
    manifest_file = data_dir / "_manifests" / "master_data.json"
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))["corrupted_records"] if manifest_file.exists() else []
    return feeds, manifest


# ---------------------------------------------------------------------------
# Order line generation
# ---------------------------------------------------------------------------

def _status_for(age_days: float, lead_days: int, rng: random.Random) -> str:
    """Lifecycle status from how long ago the order was placed vs. its promised lead time."""
    if rng.random() < CANCEL_RATE:
        return "CANCELLED"
    if age_days < 1:
        return "CREATED"
    if age_days < 2:
        return "PICKED"
    if age_days < lead_days + 1:
        return rng.choice(["SHIPPED", "SHIPPED", "DELIVERED"])
    return "DELIVERED"


def _clean_order_lines(order_no: int, keys: dict, rng: random.Random,
                       reference_time: datetime, window_days: int, seed: int,
                       assortments: dict[str, list[str]]) -> list[dict]:
    order_id = "ORD-" + hashlib.sha256(f"{seed}:{order_no}".encode()).hexdigest()[:12].upper()
    customer_id = rng.choice(keys["customers"])
    warehouse_id = rng.choice(keys["warehouses"])

    placed = reference_time - timedelta(seconds=rng.randint(0, window_days * 86400))
    lead_days = rng.randint(2, 7)
    promised = (placed + timedelta(days=lead_days)).date()
    age_days = (reference_time - placed).total_seconds() / 86400
    status = _status_for(age_days, lead_days, rng)
    # Status changes happen after placement; updated_at is the CDC freshness timestamp.
    updated = min(reference_time, placed + timedelta(hours=rng.uniform(0, max(1.0, min(age_days, lead_days + 1) * 24))))

    lines = []
    for line_no, product_id in enumerate(
        rng.sample(assortments[warehouse_id], min(rng.randint(1, MAX_LINES_PER_ORDER),
                                                  len(assortments[warehouse_id]))), start=1
    ):
        qty = rng.choice(LINE_QUANTITY_CHOICES)
        if status in ("SHIPPED", "DELIVERED"):
            shipped = qty if rng.random() > SHORT_SHIP_RATE else max(0, int(qty * rng.uniform(0.5, 0.95)))
        else:
            shipped = 0
        lines.append({
            "order_id": order_id,
            "order_line_number": line_no,
            "customer_id": customer_id,
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "order_date": placed.date().isoformat(),
            "promised_delivery_date": promised.isoformat(),
            "ordered_quantity": qty,
            "shipped_quantity": shipped,
            "unit_price": round(keys["price"][product_id] * rng.uniform(0.85, 1.0), 2),
            "order_status": status,
            "updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return lines


def _corrupt(line: dict, mode: str, reference_time: datetime) -> None:
    if mode == "null_order_id":
        line["order_id"] = None
    elif mode == "non_positive_quantity":
        line["ordered_quantity"] = 0
        line["shipped_quantity"] = 0
    elif mode == "negative_price":
        line["unit_price"] = -abs(line["unit_price"])
    elif mode == "promised_before_order":
        line["promised_delivery_date"] = (
            datetime.fromisoformat(line["order_date"]) - timedelta(days=3)).date().isoformat()
    elif mode == "shipped_exceeds_ordered":
        line["shipped_quantity"] = line["ordered_quantity"] + 5
    elif mode == "future_order_date":
        # Far enough ahead that the rule stays deterministic for years of test runs.
        future = (reference_time + timedelta(days=3650)).date()
        line["order_date"] = future.isoformat()
        line["promised_delivery_date"] = (future + timedelta(days=3)).isoformat()
    elif mode == "unknown_customer":
        line["customer_id"] = UNKNOWN_CUSTOMER_ID
    elif mode == "unknown_product":
        line["product_id"] = UNKNOWN_PRODUCT_ID
    elif mode == "unknown_warehouse":
        line["warehouse_id"] = UNKNOWN_WAREHOUSE_ID
    else:
        raise ValueError(f"Unknown failure mode {mode}")


def generate_order_lines(
    keys: dict,
    order_count: int,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    reference_time: datetime = DEFAULT_REFERENCE_DATETIME,
    window_days: int = DEFAULT_WINDOW_DAYS,
    manifest: list | None = None,
    assortment_size: int = DEFAULT_ASSORTMENT_SIZE,
) -> Iterator[dict]:
    """Yield order lines one by one (constant memory at any scale).

    Each order's products come from its warehouse's assortment (warehouse_assortment), which
    must use the same assortment_size and seed as generate_inventory.

    Each line is corrupted with probability bad_rate; the failure mode rotates through
    FAILURE_MODES. If `manifest` is a list, one entry per corruption is appended to it.
    """
    rng = random.Random(f"{seed}:orders")
    bad_rng = random.Random(f"{seed}:orders:bad")
    next_mode = 0
    assortments = {wh: warehouse_assortment(keys, wh, assortment_size, seed) for wh in keys["warehouses"]}
    for order_no in range(order_count):
        for line in _clean_order_lines(order_no, keys, rng, reference_time, window_days, seed, assortments):
            if bad_rng.random() >= bad_rate:
                yield line
                continue
            mode = FAILURE_MODES[next_mode % len(FAILURE_MODES)]
            next_mode += 1
            entry = {"feed": "orders", "failure_mode": mode,
                     "order_id": line["order_id"], "order_line_number": line["order_line_number"]}
            if mode == "duplicate":
                yield line
                yield copy.deepcopy(line)
            else:
                _corrupt(line, mode, reference_time)
                yield line
            if manifest is not None:
                manifest.append(entry)


# ---------------------------------------------------------------------------
# Pipeline Runner
# ---------------------------------------------------------------------------

def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    order_count: int = SCALE_PRESETS["dev"],
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    reference_time: datetime = DEFAULT_REFERENCE_DATETIME,
    assortment_size: int = DEFAULT_ASSORTMENT_SIZE,
) -> int:
    """Generate order lines to data/orders/orders.jsonl plus the injection manifest.

    Returns the number of lines written.
    """
    print("=" * 60)
    print("generate_orders.py – Synthetic SQL Server OMS Order Line Generator")
    print("=" * 60)
    feeds, master_manifest = load_master_data(data_dir)
    keys = usable_master_keys(feeds, master_manifest)
    print(f"Usable master keys: {len(keys['customers'])} customers, "
          f"{len(keys['products'])} products, {len(keys['warehouses'])} warehouses")

    output_file = data_dir / "orders" / "orders.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    manifest: list = []
    written = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for line in generate_order_lines(keys, order_count, seed, bad_rate, reference_time,
                                         manifest=manifest, assortment_size=assortment_size):
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
            written += 1

    manifest_file = data_dir / "_manifests" / "orders.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps({
        "seed": seed,
        "bad_rate": bad_rate,
        "orders": order_count,
        "assortment_size": assortment_size,
        "lines_written": written,
        "reference_time": reference_time.isoformat(),
        "corrupted_records": manifest,
    }, indent=2), encoding="utf-8")

    print(f"Wrote {written} order lines for {order_count} orders ({len(manifest)} corrupted) -> {output_file}")
    print(f"Injection manifest -> {manifest_file}")
    print("=" * 60)
    return written

def parse_reference_time(value: str) -> datetime:
    """'now' or an ISO timestamp such as 2026-10-20T06:00:00Z; always returned in UTC."""
    if value == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)



def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic SQL Server OMS order lines.")
    parser.add_argument("--scale", choices=sorted(SCALE_PRESETS), default="dev",
                        help="Volume preset (default: dev). --orders overrides it.")
    parser.add_argument("--orders", type=int, default=None, help="Number of orders (lines average ~3 per order)")
    parser.add_argument("--skus-per-warehouse", type=int, default=None,
                        help="Warehouse assortment size; must match generate_inventory (default: per --scale)")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_RATE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--reference-time", type=parse_reference_time, default=DEFAULT_REFERENCE_DATETIME,
                        help="'now' or an ISO timestamp (UTC): the moment the data is generated up to "
                             "(default: the fixed 2026-10-12 used by the tests)")
    args = parser.parse_args()

    order_count = args.orders if args.orders is not None else SCALE_PRESETS[args.scale]
    assortment = args.skus_per_warehouse if args.skus_per_warehouse is not None else ASSORTMENT_PRESETS[args.scale]
    try:
        if order_count < 0:
            raise ValueError("--orders must be non-negative.")
        if not 0 <= args.bad_rate < 0.5:
            raise ValueError("--bad-rate must be in [0, 0.5).")
        if assortment < 1:
            raise ValueError("--skus-per-warehouse must be >= 1.")
        run(data_dir=args.data_dir, order_count=order_count, seed=args.seed, bad_rate=args.bad_rate,
            reference_time=args.reference_time, assortment_size=assortment)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
