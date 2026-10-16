"""
generate_shipments.py – Generate carrier shipments for the orders that have left the warehouse.

One shipment (single leg) per order whose status is SHIPPED or DELIVERED in
data/orders/orders.jsonl. Each shipment departs from the order's warehouse on a route that
starts there (data/routes/routes_synthetic.jsonl) with a carrier from the carrier master:

    shipment_id, order_id, warehouse_id, route_id, carrier_id, dispatch_time_utc,
    planned_delivery_time_utc, actual_delivery_time_utc, shipment_status, ordered_quantity,
    shipped_quantity, weather_risk, data_type, updated_at

planned_delivery_time_utc is the carrier's promised ETA; OTIF compares the actual delivery
with it. Whether a shipment is late depends on the carrier's contracted OTIF target and on
weather risk, which uses the implementation guide's Open-Meteo thresholds applied to
sampled conditions (real forecasts exist for only one cached location).

Only clean inputs are used: orders with any corrupted line (data/_manifests/orders.json)
and master records marked damaged (data/_manifests/master_data.json) are skipped, so every
failure comes from this generator's own logged injection (data/_manifests/shipments.json).

Usage:
    python -m data_generators.generate_shipments
    python -m data_generators.generate_shipments --seed 7 --bad-rate 0.03
"""

import argparse
import copy
import hashlib
import itertools
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
# Must match generate_orders: orders' statuses are relative to this moment.
DEFAULT_REFERENCE_DATETIME = datetime(2026, 10, 12, 0, 0, 0, tzinfo=timezone.utc)

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Assumptions (documented, not derived)
HANDLING_HOURS = 2.0               # loading/unloading added to the route's driving time
ETA_BUFFER_RANGE = (1.10, 1.30)    # carrier pads driving time when quoting the ETA
WEATHER_PENALTY = {"LOW": 0.0, "MODERATE": 0.10, "HIGH": 0.25}   # drop in on-time probability
MEAN_LATE_HOURS = 6.0

UNKNOWN_CARRIER_ID = "CARRIER_UNKNOWN_999"
UNKNOWN_ROUTE_ID = "route_unknown_000"
UNKNOWN_WAREHOUSE_ID = "WH-9999"

FAILURE_MODES = [
    "null_shipment_id",
    "unparseable_dispatch",
    "planned_before_dispatch",
    "actual_before_dispatch",
    "delivered_without_timestamp",
    "shipped_exceeds_ordered",
    "unknown_carrier",
    "unknown_route",
    "unknown_warehouse",
    "duplicate",
]


# ---------------------------------------------------------------------------
# Weather risk (implementation guide §5.2 thresholds)
# ---------------------------------------------------------------------------

def weather_risk(precipitation_mm_h: float, wind_kmh: float, visibility_km: float) -> str:
    """HIGH if >7.5 mm/h rain, >50 km/h wind or <1 km visibility; MODERATE if >2.5 mm/h or >30 km/h."""
    if precipitation_mm_h > 7.5 or wind_kmh > 50 or visibility_km < 1:
        return "HIGH"
    if precipitation_mm_h > 2.5 or wind_kmh > 30:
        return "MODERATE"
    return "LOW"


def _sample_weather(rng: random.Random) -> str:
    precipitation = rng.expovariate(1 / 1.2)
    wind = max(0.0, rng.gauss(18, 10))
    visibility = 0.5 if rng.random() < 0.02 else rng.uniform(4, 30)
    return weather_risk(precipitation, wind, visibility)


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> Iterator[dict]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run generate_master_data and generate_orders first.")
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _manifest(data_dir: Path, name: str) -> list[dict]:
    path = data_dir / "_manifests" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))["corrupted_records"] if path.exists() else []


def build_network(carriers: list[dict], warehouses: list[dict], routes: list[dict],
                  master_manifest: list[dict]) -> dict:
    """Usable carriers (with contracted OTIF target) and routes grouped by origin warehouse."""
    master_damage = {(m["feed"], m["original_key"]) for m in master_manifest if m["failure_mode"] != "duplicate"}

    usable_carriers = {}
    for c in carriers:
        if c.get("carrier_id") and ("carriers", c["carrier_id"]) not in master_damage:
            usable_carriers[c["carrier_id"]] = c.get("contracted_otif_target") or 92.0

    origin_to_warehouse = {}
    for w in warehouses:
        if w.get("warehouse_id") and ("warehouses", w["warehouse_id"]) not in master_damage:
            origin_to_warehouse[(w["longitude"], w["latitude"])] = w["warehouse_id"]

    routes_by_warehouse: dict[str, dict] = {}
    for r in routes:
        if not r.get("route_id") or ("routes", r["route_id"]) in master_damage:
            continue
        if not ((r.get("distance_meters") or 0) > 0 and (r.get("duration_seconds") or 0) > 0):
            continue
        wh = origin_to_warehouse.get((r["origin_longitude"], r["origin_latitude"]))
        if wh:
            routes_by_warehouse.setdefault(wh, {})[r["route_id"]] = r   # dedupe repeated route rows

    if not usable_carriers or not routes_by_warehouse:
        raise ValueError("No usable carriers or routes. Run generate_master_data first.")
    return {
        "carriers": dict(sorted(usable_carriers.items())),
        "routes": {wh: sorted(rs.values(), key=lambda r: r["route_id"])
                   for wh, rs in sorted(routes_by_warehouse.items())},
    }


def load_network(data_dir: Path) -> dict:
    """build_network() over the master data files in data_dir."""
    return build_network(
        list(_read_jsonl(data_dir / "carriers" / "carriers.jsonl")),
        list(_read_jsonl(data_dir / "warehouses" / "warehouses.jsonl")),
        list(_read_jsonl(data_dir / "routes" / "routes_synthetic.jsonl")),
        _manifest(data_dir, "master_data"),
    )


def shippable_orders(order_lines: Iterator[dict], damaged_order_ids: set) -> Iterator[dict]:
    """Collapse consecutive order lines into SHIPPED/DELIVERED orders, skipping damaged orders.

    Relies on generate_orders writing each order's lines consecutively.
    """
    for order_id, group in itertools.groupby(order_lines, key=lambda l: l.get("order_id")):
        if order_id is None or order_id in damaged_order_ids:
            continue
        lines = {(l["order_line_number"]): l for l in group}.values()   # drop exact duplicate copies
        first = next(iter(lines))
        if first["order_status"] not in ("SHIPPED", "DELIVERED"):
            continue
        yield {
            "order_id": order_id,
            "warehouse_id": first["warehouse_id"],
            "order_date": first["order_date"],
            "order_status": first["order_status"],
            "ordered_quantity": sum(l["ordered_quantity"] for l in lines),
            "shipped_quantity": sum(l["shipped_quantity"] for l in lines),
        }


# ---------------------------------------------------------------------------
# Shipment generation
# ---------------------------------------------------------------------------

def _clean_shipment(order: dict, network: dict, rng: random.Random, seed: int,
                    reference_time: datetime) -> dict | None:
    routes = network["routes"].get(order["warehouse_id"])
    if not routes:
        return None
    route = rng.choice(routes)
    carrier_id = rng.choice(list(network["carriers"]))
    risk = _sample_weather(rng)

    order_day = datetime.fromisoformat(order["order_date"]).replace(tzinfo=timezone.utc)
    latest_dispatch = reference_time - timedelta(hours=2)
    dispatch = min(order_day + timedelta(hours=rng.uniform(14, 30)), latest_dispatch)
    drive_h = route["duration_seconds"] / 3600
    planned = dispatch + timedelta(hours=drive_h * rng.uniform(*ETA_BUFFER_RANGE) + HANDLING_HOURS)

    actual = None
    if order["order_status"] == "DELIVERED":
        p_on_time = network["carriers"][carrier_id] / 100 - rng.uniform(0, 0.08) - WEATHER_PENALTY[risk]
        if rng.random() < p_on_time:
            actual = planned - timedelta(hours=rng.uniform(0, min(3.0, drive_h * 0.2)))
        else:
            actual = planned + timedelta(hours=0.25 + rng.expovariate(1 / MEAN_LATE_HOURS))
        # Delivered by the reference time (the order already says DELIVERED), never before dispatch
        actual = max(dispatch + timedelta(minutes=30), min(actual, reference_time - timedelta(minutes=30)))

    return {
        "shipment_id": "SHP-" + hashlib.sha256(f"{seed}:{order['order_id']}".encode()).hexdigest()[:12].upper(),
        "order_id": order["order_id"],
        "warehouse_id": order["warehouse_id"],
        "route_id": route["route_id"],
        "carrier_id": carrier_id,
        "dispatch_time_utc": dispatch.strftime(TS_FORMAT),
        "planned_delivery_time_utc": planned.strftime(TS_FORMAT),
        "actual_delivery_time_utc": actual.strftime(TS_FORMAT) if actual else None,
        "shipment_status": "DELIVERED" if actual else "IN_TRANSIT",
        "ordered_quantity": order["ordered_quantity"],
        "shipped_quantity": order["shipped_quantity"],
        "weather_risk": risk,
        "data_type": "synthetic",
        "updated_at": (actual or dispatch).strftime(TS_FORMAT),
    }


def _corrupt(s: dict, mode: str) -> None:
    dispatch = datetime.strptime(s["dispatch_time_utc"], TS_FORMAT)
    if mode == "null_shipment_id":
        s["shipment_id"] = None
    elif mode == "unparseable_dispatch":
        s["dispatch_time_utc"] = "not-a-timestamp"
    elif mode == "planned_before_dispatch":
        s["planned_delivery_time_utc"] = (dispatch - timedelta(hours=3)).strftime(TS_FORMAT)
    elif mode == "actual_before_dispatch":
        s["shipment_status"] = "DELIVERED"
        s["actual_delivery_time_utc"] = (dispatch - timedelta(hours=1)).strftime(TS_FORMAT)
    elif mode == "delivered_without_timestamp":
        s["shipment_status"] = "DELIVERED"
        s["actual_delivery_time_utc"] = None
    elif mode == "shipped_exceeds_ordered":
        s["shipped_quantity"] = s["ordered_quantity"] + 7
    elif mode == "unknown_carrier":
        s["carrier_id"] = UNKNOWN_CARRIER_ID
    elif mode == "unknown_route":
        s["route_id"] = UNKNOWN_ROUTE_ID
    elif mode == "unknown_warehouse":
        s["warehouse_id"] = UNKNOWN_WAREHOUSE_ID
    else:
        raise ValueError(f"Unknown failure mode {mode}")


def generate_shipment_records(
    orders: Iterator[dict],
    network: dict,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    reference_time: datetime = DEFAULT_REFERENCE_DATETIME,
    manifest: list | None = None,
) -> Iterator[dict]:
    """Yield one shipment per shippable order (constant memory), corrupting ~bad_rate of them."""
    rng = random.Random(f"{seed}:shipments")
    bad_rng = random.Random(f"{seed}:shipments:bad")
    next_mode = 0
    for order in orders:
        shipment = _clean_shipment(order, network, rng, seed, reference_time)
        if shipment is None:
            continue
        if bad_rng.random() >= bad_rate:
            yield shipment
            continue
        mode = FAILURE_MODES[next_mode % len(FAILURE_MODES)]
        next_mode += 1
        if manifest is not None:
            manifest.append({"feed": "shipments", "failure_mode": mode,
                             "shipment_id": shipment["shipment_id"], "order_id": shipment["order_id"]})
        if mode == "duplicate":
            yield shipment
            yield copy.deepcopy(shipment)
        else:
            _corrupt(shipment, mode)
            yield shipment


# ---------------------------------------------------------------------------
# Pipeline Runner
# ---------------------------------------------------------------------------

def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
    reference_time: datetime = DEFAULT_REFERENCE_DATETIME,
) -> int:
    """Write data/shipments/shipments.jsonl and the injection manifest. Returns rows written."""
    print("=" * 60)
    print("generate_shipments.py – Carrier Shipment Generator (from orders)")
    print("=" * 60)
    network = load_network(data_dir)
    damaged_orders = {m["order_id"] for m in _manifest(data_dir, "orders")}
    orders = shippable_orders(_read_jsonl(data_dir / "orders" / "orders.jsonl"), damaged_orders)
    print(f"{len(network['carriers'])} carriers, routes from {len(network['routes'])} warehouses")

    output_file = data_dir / "shipments" / "shipments.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    manifest: list = []
    written = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for record in generate_shipment_records(orders, network, seed, bad_rate, reference_time, manifest):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

    manifest_file = data_dir / "_manifests" / "shipments.json"
    manifest_file.write_text(json.dumps({
        "seed": seed,
        "bad_rate": bad_rate,
        "rows_written": written,
        "reference_time": reference_time.isoformat(),
        "corrupted_records": manifest,
    }, indent=2), encoding="utf-8")
    print(f"Wrote {written} shipments ({len(manifest)} corrupted) -> {output_file}")
    print(f"Injection manifest -> {manifest_file}")
    print("=" * 60)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate carrier shipments for shipped/delivered orders.")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_RATE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args()
    try:
        if not 0 <= args.bad_rate < 0.5:
            raise ValueError("--bad-rate must be in [0, 0.5).")
        run(data_dir=args.data_dir, seed=args.seed, bad_rate=args.bad_rate)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
