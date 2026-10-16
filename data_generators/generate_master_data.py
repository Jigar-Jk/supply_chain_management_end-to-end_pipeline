"""
generate_master_data.py – Generate synthetic master data for the conformed dimensions.

Produces deterministic JSONL feeds for:
- warehouses  -> bronze.warehouses -> silver.dim_warehouse  (WMS warehouse master)
- products    -> bronze.products   -> silver.dim_product    (ERP item master)
- customers   -> bronze.customers  -> silver.dim_customer   (SQL Server OMS customer master)
- carriers    -> bronze.carriers   -> silver.dim_carrier    (carrier contract master)
- routes      -> bronze.routes     -> silver.dim_route      (warehouse -> customer lanes)

Routes are expanded synthetically from warehouse/customer coordinates (haversine distance
times a road-network factor) rather than calling OpenRouteService per lane; the real cached
ORS route in data/routes/routes.jsonl is left untouched. Synthetic routes are written to a
separate routes_synthetic.jsonl in the same landing folder, so Auto Loader picks up both.

Bad-data injection: approximately --bad-rate (default 2%) of each feed is deliberately
corrupted using failure modes the Silver quality gate checks for. A manifest of exactly
which records were corrupted is written to data/_manifests/master_data.json (outside the
landing folders so Auto Loader never ingests it).

Usage:
    python -m data_generators.generate_master_data                 # dev scale
    python -m data_generators.generate_master_data --scale full    # guide-scale volumes
    python -m data_generators.generate_master_data --products 20000 --seed 7
"""

import argparse
import copy
import hashlib
import json
import math
import random
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_RANDOM_SEED = 42
DEFAULT_BAD_RATE = 0.02

# Volume presets. "full" follows the implementation guide §5.1 targets.
SCALE_PRESETS = {
    "dev":  {"warehouses": 50,  "carriers": 20, "products": 5_000,   "customers": 5_000,   "routes": 2_000},
    "full": {"warehouses": 200, "carriers": 50, "products": 500_000, "customers": 200_000, "routes": 20_000},
}

ROUTING_PROFILE = "driving-hgv"
# Assumptions for synthetic lanes (documented, not derived): road distance is ~1.25x the
# great-circle distance; HGV average speed is 40 km/h under 50 km and 65 km/h above.
ROAD_NETWORK_FACTOR = 1.25
URBAN_SPEED_KMH = 40.0
LONG_HAUL_SPEED_KMH = 65.0

# Real city coordinates (lat, lon). The ORS seed route is in Heidelberg, so the network is
# centred on Germany and neighbouring countries.
CITIES = [
    ("Heidelberg", "DE", 49.3988, 8.6724), ("Frankfurt", "DE", 50.1109, 8.6821),
    ("Mannheim", "DE", 49.4875, 8.4660), ("Stuttgart", "DE", 48.7758, 9.1829),
    ("Munich", "DE", 48.1351, 11.5820), ("Nuremberg", "DE", 49.4521, 11.0767),
    ("Cologne", "DE", 50.9375, 6.9603), ("Dusseldorf", "DE", 51.2277, 6.7735),
    ("Dortmund", "DE", 51.5136, 7.4653), ("Hamburg", "DE", 53.5511, 9.9937),
    ("Bremen", "DE", 53.0793, 8.8017), ("Hanover", "DE", 52.3759, 9.7320),
    ("Berlin", "DE", 52.5200, 13.4050), ("Leipzig", "DE", 51.3397, 12.3731),
    ("Dresden", "DE", 51.0504, 13.7373), ("Karlsruhe", "DE", 49.0069, 8.4037),
    ("Strasbourg", "FR", 48.5734, 7.7521), ("Lyon", "FR", 45.7640, 4.8357),
    ("Paris", "FR", 48.8566, 2.3522), ("Lille", "FR", 50.6292, 3.0573),
    ("Brussels", "BE", 50.8503, 4.3517), ("Antwerp", "BE", 51.2194, 4.4025),
    ("Rotterdam", "NL", 51.9244, 4.4777), ("Amsterdam", "NL", 52.3676, 4.9041),
    ("Luxembourg", "LU", 49.6116, 6.1319), ("Zurich", "CH", 47.3769, 8.5417),
    ("Basel", "CH", 47.5596, 7.5886), ("Vienna", "AT", 48.2082, 16.3738),
    ("Salzburg", "AT", 47.8095, 13.0550), ("Prague", "CZ", 50.0755, 14.4378),
    ("Milan", "IT", 45.4642, 9.1900), ("Copenhagen", "DK", 55.6761, 12.5683),
    ("Poznan", "PL", 52.4064, 16.9252), ("Warsaw", "PL", 52.2297, 21.0122),
]

FACILITY_TYPES = {
    "REGIONAL_DC": "Regional DC",
    "HUB": "Hub",
    "FULFILLMENT_CENTER": "Fulfillment Center",
    "CROSS_DOCK": "Cross-Dock",
}

# category -> (unit weight kg range, unit volume m3 range, unit cost range)
PRODUCT_CATEGORIES = {
    "ELECTRONICS": ((0.2, 15.0), (0.001, 0.08), (15.0, 900.0)),
    "APPAREL": ((0.1, 2.0), (0.001, 0.02), (4.0, 120.0)),
    "HOME_GOODS": ((0.5, 30.0), (0.005, 0.25), (6.0, 400.0)),
    "GROCERY": ((0.1, 12.0), (0.0005, 0.03), (0.5, 40.0)),
    "INDUSTRIAL": ((1.0, 120.0), (0.01, 0.6), (10.0, 1500.0)),
    "PHARMA": ((0.05, 3.0), (0.0002, 0.01), (2.0, 300.0)),
    "AUTOMOTIVE": ((0.5, 60.0), (0.002, 0.4), (8.0, 1200.0)),
}
PRODUCT_ADJECTIVES = ["Compact", "Premium", "Standard", "Heavy-Duty", "Eco", "Pro", "Classic", "Smart", "Ultra", "Basic"]
PRODUCT_NOUNS = {
    "ELECTRONICS": ["Router", "Monitor", "Headset", "Tablet", "Charger", "Speaker"],
    "APPAREL": ["Jacket", "Shirt", "Boots", "Gloves", "Trousers", "Cap"],
    "HOME_GOODS": ["Lamp", "Chair", "Kettle", "Shelf", "Rug", "Cookware Set"],
    "GROCERY": ["Coffee", "Pasta", "Olive Oil", "Cereal", "Tea", "Rice"],
    "INDUSTRIAL": ["Pump", "Valve", "Compressor", "Drill", "Pallet Jack", "Cable Drum"],
    "PHARMA": ["Bandage Pack", "Vitamin Pack", "Test Kit", "Syringe Box", "Glove Box", "Thermometer"],
    "AUTOMOTIVE": ["Brake Pad", "Battery", "Tyre", "Filter", "Wiper Set", "Spark Plug Kit"],
}

CUSTOMER_SEGMENTS = ["ENTERPRISE", "SMB", "RETAIL"]
CUSTOMER_PREFIXES = ["Nord", "Alpen", "Rhein", "Euro", "Central", "Metro", "Delta", "Atlas", "Nova", "Prime", "Union", "Vista"]
CUSTOMER_SUFFIXES = ["Retail", "Trading", "Supplies", "Distribution", "Markets", "Industries", "Wholesale", "Stores", "Group", "Partners"]
CUSTOMER_FORMS = ["GmbH", "AG", "SARL", "BV", "Ltd", "S.p.A.", "KG"]

# The first five IDs match the existing synthetic shipments feed for compatibility.
LEGACY_CARRIER_IDS = ["CARRIER_DHL", "CARRIER_FEDEX", "CARRIER_UPS", "CARRIER_DB_SCHENKER", "CARRIER_KUHNE_NAGEL"]
LEGACY_CARRIER_NAMES = {
    "CARRIER_DHL": "DHL", "CARRIER_FEDEX": "FedEx", "CARRIER_UPS": "UPS",
    "CARRIER_DB_SCHENKER": "DB Schenker", "CARRIER_KUHNE_NAGEL": "Kuehne Nagel",
}
CARRIER_SERVICE_LEVELS = ["STANDARD", "EXPRESS", "FREIGHT_HEAVY", "ECONOMY"]
CARRIER_MODES = ["ROAD", "ROAD", "ROAD", "INTERMODAL", "RAIL"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def route_id_for(origin_lon: float, origin_lat: float, dest_lon: float, dest_lat: float,
                 profile: str = ROUTING_PROFILE) -> str:
    """Deterministic route_id, using the same hash contract as normalize_api_data.normalize_route."""
    id_input = json.dumps({
        "origin": [origin_lon, origin_lat],
        "destination": [dest_lon, dest_lat],
        "profile": profile,
    }, sort_keys=True)
    return hashlib.sha256(id_input.encode()).hexdigest()[:16]


def _jitter(rng: random.Random, lat: float, lon: float, spread: float) -> tuple[float, float]:
    return round(lat + rng.uniform(-spread, spread), 6), round(lon + rng.uniform(-spread, spread), 6)


def _bad_count(n: int, bad_rate: float) -> int:
    """Number of records to corrupt: round(n * rate), at least 1 when the rate is non-zero."""
    if n <= 0 or bad_rate <= 0:
        return 0
    return max(1, int(round(n * bad_rate)))


# ---------------------------------------------------------------------------
# Clean entity generators
# ---------------------------------------------------------------------------

def generate_warehouses(count: int, rng: random.Random) -> list[dict]:
    warehouses = []
    for i in range(1, count + 1):
        city, country, lat, lon = CITIES[(i - 1) % len(CITIES)]
        w_lat, w_lon = _jitter(rng, lat, lon, 0.15)
        facility = rng.choice(list(FACILITY_TYPES))
        warehouses.append({
            "warehouse_id": f"WH-{i:04d}",
            "warehouse_name": f"{city} {FACILITY_TYPES[facility]} {i:04d}",
            "facility_type": facility,
            "city": city,
            "country": country,
            "latitude": w_lat,
            "longitude": w_lon,
            "capacity_sqm": float(rng.randrange(5_000, 80_001, 500)),
        })
    return warehouses


def generate_products(count: int, rng: random.Random) -> list[dict]:
    categories = list(PRODUCT_CATEGORIES)
    products = []
    for i in range(1, count + 1):
        category = rng.choice(categories)
        (w_lo, w_hi), (v_lo, v_hi), (c_lo, c_hi) = PRODUCT_CATEGORIES[category]
        unit_cost = round(rng.uniform(c_lo, c_hi), 2)
        products.append({
            "product_id": f"SKU-{i:06d}",
            "product_name": f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS[category])}",
            "category": category,
            "unit_weight_kg": round(rng.uniform(w_lo, w_hi), 3),
            "unit_volume_cbm": round(rng.uniform(v_lo, v_hi), 5),
            "unit_cost": unit_cost,
            "unit_price": round(unit_cost * rng.uniform(1.15, 1.8), 2),
        })
    return products


def generate_customers(count: int, rng: random.Random) -> list[dict]:
    customers = []
    for i in range(1, count + 1):
        city, country, _, _ = rng.choice(CITIES)
        customers.append({
            "customer_id": f"CUST-{i:06d}",
            "customer_name": f"{rng.choice(CUSTOMER_PREFIXES)} {rng.choice(CUSTOMER_SUFFIXES)} {rng.choice(CUSTOMER_FORMS)}",
            "customer_segment": rng.choices(CUSTOMER_SEGMENTS, weights=[1, 3, 6])[0],
            "country": country,
            "city": city,
        })
    return customers


def generate_carriers(count: int, rng: random.Random) -> list[dict]:
    carriers = []
    for i in range(1, count + 1):
        if i <= len(LEGACY_CARRIER_IDS):
            carrier_id = LEGACY_CARRIER_IDS[i - 1]
        else:
            carrier_id = f"CARRIER_REGIONAL_{i:03d}"
        name_token = LEGACY_CARRIER_NAMES.get(carrier_id) or carrier_id.replace("CARRIER_", "").replace("_", " ").title()
        carriers.append({
            "carrier_id": carrier_id,
            "carrier_name": f"{name_token} Logistics",
            "service_level": rng.choice(CARRIER_SERVICE_LEVELS),
            "transport_mode": rng.choice(CARRIER_MODES),
            "contracted_otif_target": round(rng.uniform(90.0, 98.5), 1),
            "contact_email": f"{carrier_id.lower()}@carrier-network.example",
            "is_active": True,
        })
    return carriers


def generate_routes(count: int, warehouses: list[dict], customers: list[dict], rng: random.Random) -> list[dict]:
    """Generate distinct warehouse -> customer-location lanes.

    Destinations are jittered customer city points; each lane's route_id is derived from its
    coordinates with route_id_for(), so downstream generators can recompute it.
    """
    if not warehouses or not customers:
        raise ValueError("Routes need at least one warehouse and one customer.")
    routes: dict[str, dict] = {}
    max_attempts = count * 20
    attempts = 0
    while len(routes) < count and attempts < max_attempts:
        attempts += 1
        wh = rng.choice(warehouses)
        cust = rng.choice(customers)
        city = next(c for c in CITIES if c[0] == cust["city"])
        d_lat, d_lon = _jitter(rng, city[2], city[3], 0.08)
        route_id = route_id_for(wh["longitude"], wh["latitude"], d_lon, d_lat)
        if route_id in routes:
            continue
        km = max(2.0, haversine_km(wh["latitude"], wh["longitude"], d_lat, d_lon) * ROAD_NETWORK_FACTOR)
        speed = URBAN_SPEED_KMH if km < 50 else LONG_HAUL_SPEED_KMH
        routes[route_id] = {
            "route_id": route_id,
            "origin_longitude": wh["longitude"],
            "origin_latitude": wh["latitude"],
            "destination_longitude": d_lon,
            "destination_latitude": d_lat,
            "distance_meters": round(km * 1000, 1),
            "duration_seconds": round(km / speed * 3600, 1),
            "routing_profile": ROUTING_PROFILE,
            "source": "synthetic_expansion",
        }
    return list(routes.values())


# ---------------------------------------------------------------------------
# Bad-data injection
# ---------------------------------------------------------------------------

def _corrupt_null_key(rec: dict, key: str) -> None:
    rec[key] = None


# feed -> (natural key, [(failure_mode, mutator)]). "duplicate" is handled separately.
CORRUPTIONS = {
    "warehouses": ("warehouse_id", [
        ("null_key", lambda r: _corrupt_null_key(r, "warehouse_id")),
        ("latitude_out_of_range", lambda r: r.update(latitude=999.0)),
        ("duplicate", None),
    ]),
    "products": ("product_id", [
        ("null_key", lambda r: _corrupt_null_key(r, "product_id")),
        ("non_positive_weight", lambda r: r.update(unit_weight_kg=-abs(r["unit_weight_kg"]))),
        ("zero_volume", lambda r: r.update(unit_volume_cbm=0.0)),
        ("duplicate", None),
    ]),
    "customers": ("customer_id", [
        ("empty_key", lambda r: r.update(customer_id="")),
        ("null_key", lambda r: _corrupt_null_key(r, "customer_id")),
        ("duplicate", None),
    ]),
    "carriers": ("carrier_id", [
        ("null_key", lambda r: _corrupt_null_key(r, "carrier_id")),
        ("empty_key", lambda r: r.update(carrier_id="")),
    ]),
    "routes": ("route_id", [
        ("negative_distance", lambda r: r.update(distance_meters=-abs(r["distance_meters"]))),
        ("latitude_out_of_range", lambda r: r.update(origin_latitude=123.0)),
        ("zero_duration", lambda r: r.update(duration_seconds=0.0)),
        ("null_key", lambda r: _corrupt_null_key(r, "route_id")),
        ("duplicate", None),
    ]),
}


def inject_bad_records(feed: str, records: list[dict], bad_rate: float, rng: random.Random) -> tuple[list[dict], list[dict]]:
    """Corrupt ~bad_rate of records in place (duplicates are appended copies).

    Returns (records, manifest_entries). Failure modes are assigned round-robin so every mode
    appears once the feed is large enough. Each manifest entry names the original key so a
    reviewer can trace exactly which entity was damaged.
    """
    key, modes = CORRUPTIONS[feed]
    n_bad = _bad_count(len(records), bad_rate)
    if n_bad == 0:
        return records, []

    victims = rng.sample(range(len(records)), min(n_bad, len(records)))
    manifest = []
    duplicates = []
    for j, idx in enumerate(sorted(victims)):
        mode, mutate = modes[j % len(modes)]
        original_key = records[idx][key]
        if mutate is None:
            duplicates.append(copy.deepcopy(records[idx]))
        else:
            mutate(records[idx])
        manifest.append({"feed": feed, "failure_mode": mode, "original_key": original_key})

    return records + duplicates, manifest


# ---------------------------------------------------------------------------
# File Writing & Pipeline Runner
# ---------------------------------------------------------------------------

def write_jsonl(records: list[dict], output_file: Path) -> None:
    """Write records as NDJSON, replacing existing content."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_master_data(
    counts: dict,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
) -> tuple[dict[str, list[dict]], list[dict]]:
    """Generate all master feeds. Returns ({feed: records}, manifest).

    Routes are generated from the *clean* warehouses and customers, before injection, so lane
    geometry is always plausible; route corruption is then applied to the routes feed itself.
    Each feed uses its own derived RNG so changing one volume does not reshuffle the others.
    """
    def rng_for(feed: str) -> random.Random:
        return random.Random(f"{seed}:{feed}")

    warehouses = generate_warehouses(counts["warehouses"], rng_for("warehouses"))
    products = generate_products(counts["products"], rng_for("products"))
    customers = generate_customers(counts["customers"], rng_for("customers"))
    carriers = generate_carriers(counts["carriers"], rng_for("carriers"))
    routes = generate_routes(counts["routes"], warehouses, customers, rng_for("routes"))

    feeds = {
        "warehouses": warehouses,
        "products": products,
        "customers": customers,
        "carriers": carriers,
        "routes": routes,
    }
    manifest: list[dict] = []
    for feed in feeds:
        feeds[feed], entries = inject_bad_records(feed, feeds[feed], bad_rate, rng_for(f"{feed}:bad"))
        manifest.extend(entries)
    return feeds, manifest


OUTPUT_FILES = {
    "warehouses": Path("warehouses") / "warehouses.jsonl",
    "products": Path("products") / "products.jsonl",
    "customers": Path("customers") / "customers.jsonl",
    "carriers": Path("carriers") / "carriers.jsonl",
    "routes": Path("routes") / "routes_synthetic.jsonl",
}


def run(
    data_dir: Path = DEFAULT_DATA_DIR,
    counts: dict | None = None,
    seed: int = DEFAULT_RANDOM_SEED,
    bad_rate: float = DEFAULT_BAD_RATE,
) -> dict[str, int]:
    """Generate and write all master data feeds plus the injection manifest.

    Returns {feed: rows written}.
    """
    counts = counts or SCALE_PRESETS["dev"]
    print("=" * 60)
    print("generate_master_data.py – Synthetic Master Data Generator")
    print("=" * 60)
    print(f"Seed={seed}  bad_rate={bad_rate:.1%}  counts={counts}")

    feeds, manifest = build_master_data(counts, seed=seed, bad_rate=bad_rate)

    written = {}
    for feed, records in feeds.items():
        out = data_dir / OUTPUT_FILES[feed]
        write_jsonl(records, out)
        written[feed] = len(records)
        n_bad = sum(1 for m in manifest if m["feed"] == feed)
        print(f"  {feed:<11} {len(records):>8} rows ({n_bad} corrupted) -> {out}")

    manifest_file = data_dir / "_manifests" / "master_data.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps({
        "seed": seed,
        "bad_rate": bad_rate,
        "counts": counts,
        "rows_written": written,
        "corrupted_records": manifest,
    }, indent=2), encoding="utf-8")
    print(f"Injection manifest -> {manifest_file}")
    print("=" * 60)
    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic master data for the conformed dimensions.")
    parser.add_argument("--scale", choices=sorted(SCALE_PRESETS), default="dev",
                        help="Volume preset (default: dev). Individual --<feed> flags override it.")
    for feed in SCALE_PRESETS["dev"]:
        parser.add_argument(f"--{feed}", type=int, default=None, help=f"Override number of {feed}")
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED,
                        help=f"Random seed for reproducibility (default: {DEFAULT_RANDOM_SEED})")
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_RATE,
                        help=f"Fraction of records to corrupt per feed (default: {DEFAULT_BAD_RATE})")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Root data directory")
    args = parser.parse_args()

    counts = dict(SCALE_PRESETS[args.scale])
    for feed in counts:
        override = getattr(args, feed)
        if override is not None:
            counts[feed] = override

    try:
        if any(v < 0 for v in counts.values()):
            raise ValueError("Counts must be non-negative.")
        if not 0 <= args.bad_rate < 0.5:
            raise ValueError("--bad-rate must be in [0, 0.5).")
        run(data_dir=args.data_dir, counts=counts, seed=args.seed, bad_rate=args.bad_rate)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
