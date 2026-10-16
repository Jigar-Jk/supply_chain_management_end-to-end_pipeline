"""
generate_inventory.py – Generate the WMS nightly stock reconciliation feed (NDJSON).

One file per night, data/inventory/inventory_<YYYY-MM-DD>.jsonl, one row per
warehouse x product x night (the silver.fact_inventory_snapshot grain):
    warehouse_id, product_id, snapshot_date, quantity_on_hand, quantity_reserved,
    safety_stock, reorder_point, recorded_at

Each warehouse stocks the assortment from generate_orders.warehouse_assortment (the same
products its orders are filled from; use the same --skus-per-warehouse and --seed for both).
Stock is simulated night to
night: daily demand draws it down; when it falls to the reorder point a replenishment
usually arrives, but sometimes it is late, so genuine stockouts (available <= 0) and
stockout risks (available <= safety stock) appear for the stockout-rate KPI.

Only master records expected to pass the Silver quality gate are referenced (entities the
master manifest marks as damaged are excluded), so every failure is this generator's own,
explicitly logged injection. Manifest: data/_manifests/inventory.json.

Usage:
    python -m data_generators.generate_inventory                 # dev: 3 nights x 500 SKUs/warehouse
    python -m data_generators.generate_inventory --scale full    # ~3M rows for one night
    python -m data_generators.generate_inventory --nights 7 --skus-per-warehouse 200
"""

import argparse
import copy
import json
import math
import random
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from data_generators.generate_orders import (
    ASSORTMENT_PRESETS,
    DEFAULT_WINDOW_DAYS,
    SCALE_PRESETS as ORDER_SCALE_PRESETS,
    expected_daily_units_per_sku,
    load_master_data,
    usable_master_keys,
    warehouse_assortment,
)

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_RANDOM_SEED = 42
DEFAULT_BAD_RATE = 0.02
# Last reconciliation night; nights run backwards from here.
DEFAULT_LAST_SNAPSHOT_DATE = date(2026, 10, 12)

SCALE_PRESETS = {
    "dev": {"nights": 3, "skus_per_warehouse": ASSORTMENT_PRESETS["dev"]},
    "full": {"nights": 1, "skus_per_warehouse": ASSORTMENT_PRESETS["full"]},   # 200 warehouses -> ~3M rows/night
}

# Simulation assumptions (documented, not derived)
# Typical DC policy: 10 days safety stock, 14-day replenishment lead time, replenish up to
# 60 days of cover -> ~30 days on hand on average -> ~12 turns a year.
REPLENISH_ON_TIME_RATE = 0.35   # chance a due replenishment actually arrives that night
SAFETY_STOCK_DAYS = 10
REORDER_EXTRA_DAYS = 14         # reorder point = safety stock + lead-time demand
ORDER_UP_TO_DAYS = 60
START_OUT_OF_STOCK_RATE = 0.04  # SKUs already out at the first night (late replenishment in progress)
# Per-SKU demand varies around the mean implied by the order volume (multipliers average 1.0)
DEMAND_MULTIPLIERS = [0.25, 0.5, 0.75, 1.0, 1.0, 1.25, 1.5, 1.75]

# Unknown codes are derived from the original key (like a typo or a retired code), so every corrupted
# row keeps a UNIQUE warehouse x product x date key. One shared code would make several bad rows
# collide on one key, and Silver would treat all but the newest as superseded versions.
UNKNOWN_WAREHOUSE_PREFIX = "WH-UNKNOWN-"
UNKNOWN_PRODUCT_PREFIX = "SKU-UNKNOWN-"

FAILURE_MODES = [
    "null_warehouse_id",
    "null_product_id",
    "negative_on_hand",
    "reserved_exceeds_on_hand",
    "future_snapshot_date",
    "unknown_warehouse",
    "unknown_product",
    "duplicate",
]


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def _simulate_sku(rng: random.Random, nights: list[date], mean_daily_units: float) -> list[dict]:
    """Simulate one warehouse x product across consecutive nights.

    Daily demand is drawn around mean_daily_units (the rate the orders feed actually sells),
    so stock levels and COGS describe the same business.
    """
    daily_demand = mean_daily_units * rng.choice(DEMAND_MULTIPLIERS)
    safety = max(1, math.ceil(daily_demand * SAFETY_STOCK_DAYS))
    reorder = safety + max(1, math.ceil(daily_demand * REORDER_EXTRA_DAYS))
    order_up_to = max(reorder + 1, math.ceil(daily_demand * ORDER_UP_TO_DAYS))
    # Start anywhere between one day of cover and the order-up-to level; a few SKUs start
    # already out, so even a short window of nights shows realistic stockouts.
    if rng.random() < START_OUT_OF_STOCK_RATE:
        on_hand = 0
    else:
        on_hand = rng.randint(max(1, math.ceil(daily_demand)), order_up_to)

    states = []
    for _ in nights:
        on_hand = max(0, on_hand - int(round(daily_demand * rng.uniform(0.3, 1.8))))
        if on_hand <= reorder and rng.random() < REPLENISH_ON_TIME_RATE:
            on_hand = order_up_to + rng.randint(0, max(1, math.ceil(daily_demand * 2)))
        reserved = 0 if on_hand == 0 else min(on_hand, int(on_hand * rng.uniform(0.0, 0.35)))
        # Occasionally everything on hand is allocated: available = 0 without on_hand = 0
        if on_hand and rng.random() < 0.01:
            reserved = on_hand
        states.append({"on_hand": on_hand, "reserved": reserved, "safety": safety, "reorder": reorder})
    return states


def _corrupt(row: dict, mode: str) -> None:
    if mode == "null_warehouse_id":
        row["warehouse_id"] = None
    elif mode == "null_product_id":
        row["product_id"] = None
    elif mode == "negative_on_hand":
        row["quantity_on_hand"] = -max(1, row["quantity_on_hand"])
        row["quantity_reserved"] = 0
    elif mode == "reserved_exceeds_on_hand":
        row["quantity_reserved"] = row["quantity_on_hand"] + 10
    elif mode == "future_snapshot_date":
        # Far ahead so the rule stays deterministic for years of test runs.
        row["snapshot_date"] = (date.fromisoformat(row["snapshot_date"]) + timedelta(days=3650)).isoformat()
    elif mode == "unknown_warehouse":
        row["warehouse_id"] = UNKNOWN_WAREHOUSE_PREFIX + row["warehouse_id"].split("-", 1)[-1]
    elif mode == "unknown_product":
        row["product_id"] = UNKNOWN_PRODUCT_PREFIX + row["product_id"].split("-", 1)[-1]
    else:
        raise ValueError(f"Unknown failure mode {mode}")


def generate_inventory_rows(
    keys: dict,
    nights: list[date],
    skus_per_warehouse: int,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    manifest: list | None = None,
    mean_daily_units: float = 1.0,
) -> Iterator[dict]:
    """Yield snapshot rows warehouse by warehouse (memory bounded by one warehouse).

    Rows for all nights of one warehouse x product are yielded together; callers route each
    row to its night's file via row["snapshot_date"] (a corrupted future date stays in the
    original night's file, as a real bad export would).
    """
    bad_rng = random.Random(f"{seed}:inventory:bad")
    next_mode = 0
    for warehouse_id in keys["warehouses"]:
        rng = random.Random(f"{seed}:inventory:{warehouse_id}")
        for product_id in warehouse_assortment(keys, warehouse_id, skus_per_warehouse, seed):
            for night, state in zip(nights, _simulate_sku(rng, nights, mean_daily_units)):
                row = {
                    "warehouse_id": warehouse_id,
                    "product_id": product_id,
                    "snapshot_date": night.isoformat(),
                    "quantity_on_hand": state["on_hand"],
                    "quantity_reserved": state["reserved"],
                    "safety_stock": state["safety"],
                    "reorder_point": state["reorder"],
                    "recorded_at": f"{night.isoformat()}T01:{rng.randint(0, 59):02d}:00Z",
                }
                file_night = row["snapshot_date"]
                if bad_rng.random() >= bad_rate:
                    yield file_night, row
                    continue
                mode = FAILURE_MODES[next_mode % len(FAILURE_MODES)]
                next_mode += 1
                if manifest is not None:
                    manifest.append({"feed": "inventory", "failure_mode": mode, "warehouse_id": warehouse_id,
                                     "product_id": product_id, "snapshot_date": file_night})
                if mode == "duplicate":
                    yield file_night, row
                    yield file_night, copy.deepcopy(row)
                else:
                    _corrupt(row, mode)
                    yield file_night, row


def snapshot_nights(last_night: date, count: int) -> list[date]:
    """`count` consecutive nights ending at last_night, oldest first."""
    return [last_night - timedelta(days=i) for i in reversed(range(count))]


# ---------------------------------------------------------------------------
# Pipeline Runner
# ---------------------------------------------------------------------------

def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    nights: int = SCALE_PRESETS["dev"]["nights"],
    skus_per_warehouse: int = SCALE_PRESETS["dev"]["skus_per_warehouse"],
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    last_night: date = DEFAULT_LAST_SNAPSHOT_DATE,
    mean_daily_units: float | None = None,
) -> dict[str, int]:
    """Write one NDJSON file per night plus the injection manifest. Returns {night: rows}.

    mean_daily_units defaults to the demand implied by the orders feed already generated in
    data_dir (data/_manifests/orders.json), else by the dev order volume.
    """
    print("=" * 60)
    print("generate_inventory.py – WMS Nightly Stock Reconciliation Generator")
    print("=" * 60)
    feeds, master_manifest = load_master_data(data_dir)
    keys = usable_master_keys(feeds, master_manifest)
    night_list = snapshot_nights(last_night, nights)
    if mean_daily_units is None:
        orders_manifest = data_dir / "_manifests" / "orders.json"
        order_count = (json.loads(orders_manifest.read_text(encoding="utf-8"))["orders"]
                       if orders_manifest.exists() else ORDER_SCALE_PRESETS["dev"])
        mean_daily_units = expected_daily_units_per_sku(
            order_count, len(keys["warehouses"]), skus_per_warehouse, DEFAULT_WINDOW_DAYS)
    print(f"Mean demand per warehouse x SKU: {mean_daily_units:.3f} units/day")
    print(f"{len(keys['warehouses'])} warehouses x {skus_per_warehouse} SKUs x {nights} night(s)")

    out_dir = data_dir / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)
    handles = {n.isoformat(): open(out_dir / f"inventory_{n.isoformat()}.jsonl", "w", encoding="utf-8")
               for n in night_list}
    written = {n: 0 for n in handles}
    manifest: list = []
    try:
        for night, row in generate_inventory_rows(keys, night_list, skus_per_warehouse, seed, bad_rate, manifest,
                                                  mean_daily_units=mean_daily_units):
            handles[night].write(json.dumps(row, ensure_ascii=False) + "\n")
            written[night] += 1
    finally:
        for h in handles.values():
            h.close()

    manifest_file = data_dir / "_manifests" / "inventory.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps({
        "seed": seed,
        "bad_rate": bad_rate,
        "nights": [n.isoformat() for n in night_list],
        "skus_per_warehouse": skus_per_warehouse,
        "mean_daily_units": mean_daily_units,
        "rows_written": written,
        "corrupted_records": manifest,
    }, indent=2), encoding="utf-8")

    for night, n in written.items():
        print(f"  {night}: {n} rows -> {out_dir / f'inventory_{night}.jsonl'}")
    print(f"{len(manifest)} corrupted rows; manifest -> {manifest_file}")
    print("=" * 60)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the WMS nightly stock reconciliation NDJSON feed.")
    parser.add_argument("--scale", choices=sorted(SCALE_PRESETS), default="dev")
    parser.add_argument("--nights", type=int, default=None, help="Number of consecutive nights")
    parser.add_argument("--skus-per-warehouse", type=int, default=None, help="Assortment size per warehouse")
    parser.add_argument("--last-night", type=date.fromisoformat, default=DEFAULT_LAST_SNAPSHOT_DATE,
                        help="Last snapshot date, YYYY-MM-DD (default: 2026-10-12)")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_RATE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args()

    preset = SCALE_PRESETS[args.scale]
    nights = args.nights if args.nights is not None else preset["nights"]
    skus = args.skus_per_warehouse if args.skus_per_warehouse is not None else preset["skus_per_warehouse"]
    try:
        if nights < 1 or skus < 1:
            raise ValueError("--nights and --skus-per-warehouse must be >= 1.")
        if not 0 <= args.bad_rate < 0.5:
            raise ValueError("--bad-rate must be in [0, 0.5).")
        run(data_dir=args.data_dir, nights=nights, skus_per_warehouse=skus, seed=args.seed,
            bad_rate=args.bad_rate, last_night=args.last_night)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
