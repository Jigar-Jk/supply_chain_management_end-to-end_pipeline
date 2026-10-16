"""
validate_datasets.py – Dataset Quality & Integrity Validator.

Validates the structure, data quality, and referential integrity of:
- data/routes/routes.jsonl
- data/weather/weather.jsonl
- data/shipments/shipments.jsonl

Usage:
    python -m data_generators.validate_datasets
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Default Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_ROUTES_FILE = PROJECT_ROOT / "data" / "routes" / "routes.jsonl"
DEFAULT_WEATHER_FILE = PROJECT_ROOT / "data" / "weather" / "weather.jsonl"
DEFAULT_SHIPMENTS_FILE = PROJECT_ROOT / "data" / "shipments" / "shipments.jsonl"
DEFAULT_SYNTHETIC_ROUTES_FILE = PROJECT_ROOT / "data" / "routes" / "routes_synthetic.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_iso8601(timestamp_str: str) -> datetime | None:
    """Parse an ISO 8601 timestamp string, returning datetime or None if invalid."""
    if not isinstance(timestamp_str, str) or not timestamp_str.strip():
        return None
    # Handle optional Z suffix
    ts = timestamp_str.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def is_number(val) -> bool:
    """Return True if val is int or float (and not bool)."""
    return isinstance(val, (int, float)) and not isinstance(val, bool)


# ---------------------------------------------------------------------------
# Route Validation
# ---------------------------------------------------------------------------

REQUIRED_ROUTE_FIELDS = [
    "route_id",
    "origin_longitude",
    "origin_latitude",
    "destination_longitude",
    "destination_latitude",
    "distance_meters",
    "duration_seconds",
    "routing_profile",
    "source",
]


def validate_routes(routes_file: Path) -> dict:
    """Validate route dataset.

    Returns dict with summary statistics, errors, and extracted valid route IDs.
    """
    result = {
        "status": "PASS",
        "total_records": 0,
        "valid_records": 0,
        "errors": [],
        "warnings": [],
        "route_ids": set(),
        "duplicate_route_ids": set(),
    }

    if not routes_file.exists():
        result["status"] = "FAIL"
        result["errors"].append(f"File not found: {routes_file}")
        return result

    try:
        content = routes_file.read_text(encoding="utf-8")
    except Exception as exc:
        result["status"] = "FAIL"
        result["errors"].append(f"Unable to read file {routes_file}: {exc}")
        return result

    lines = content.splitlines()
    seen_ids = set()

    for line_num, line in enumerate(lines, start=1):
        line_str = line.strip()
        if not line_str:
            continue

        result["total_records"] += 1

        try:
            record = json.loads(line_str)
        except json.JSONDecodeError as exc:
            result["errors"].append(f"Line {line_num}: Malformed JSON - {exc}")
            continue

        if not isinstance(record, dict):
            result["errors"].append(f"Line {line_num}: Record is not a JSON object.")
            continue

        record_errors = []

        # Required fields check
        for field in REQUIRED_ROUTE_FIELDS:
            if field not in record or record[field] is None:
                record_errors.append(f"Missing required field '{field}'")

        # route_id checks
        route_id = record.get("route_id")
        if route_id is not None:
            if not isinstance(route_id, str) or not route_id.strip():
                record_errors.append("'route_id' must be a non-empty string")
            else:
                if route_id in seen_ids:
                    record_errors.append(f"Duplicate 'route_id' detected: '{route_id}'")
                    result["duplicate_route_ids"].add(route_id)
                seen_ids.add(route_id)

        # Coordinate checks
        origin_lon = record.get("origin_longitude")
        origin_lat = record.get("origin_latitude")
        dest_lon = record.get("destination_longitude")
        dest_lat = record.get("destination_latitude")

        for name, val, min_val, max_val in [
            ("origin_latitude", origin_lat, -90.0, 90.0),
            ("destination_latitude", dest_lat, -90.0, 90.0),
            ("origin_longitude", origin_lon, -180.0, 180.0),
            ("destination_longitude", dest_lon, -180.0, 180.0),
        ]:
            if val is not None:
                if not is_number(val):
                    record_errors.append(f"'{name}' must be numeric (got {type(val).__name__})")
                elif not (min_val <= val <= max_val):
                    record_errors.append(f"'{name}' value {val} outside valid range [{min_val}, {max_val}]")

        # Distance & duration checks
        dist = record.get("distance_meters")
        dur = record.get("duration_seconds")

        for name, val in [("distance_meters", dist), ("duration_seconds", dur)]:
            if val is not None:
                if not is_number(val):
                    record_errors.append(f"'{name}' must be numeric (got {type(val).__name__})")
                elif val <= 0:
                    record_errors.append(f"'{name}' must be positive (got {val})")

        if record_errors:
            for err in record_errors:
                result["errors"].append(f"Line {line_num}: {err}")
        else:
            result["valid_records"] += 1
            if route_id:
                result["route_ids"].add(route_id)

    if result["total_records"] == 0:
        result["errors"].append("Route file contains no records.")

    if result["errors"]:
        result["status"] = "FAIL"

    return result


# ---------------------------------------------------------------------------
# Weather Validation
# ---------------------------------------------------------------------------

REQUIRED_WEATHER_FIELDS = [
    "weather_record_id",
    "latitude",
    "longitude",
    "forecast_time_utc",
    "temperature_2m_c",
    "precipitation_mm",
    "wind_speed_10m_kmh",
    "visibility_m",
    "source",
]


def validate_weather(weather_file: Path) -> dict:
    """Validate weather dataset.

    Returns dict with summary statistics and errors.
    """
    result = {
        "status": "PASS",
        "total_records": 0,
        "valid_records": 0,
        "errors": [],
        "warnings": [],
        "weather_record_ids": set(),
        "duplicate_weather_ids": set(),
    }

    if not weather_file.exists():
        result["status"] = "FAIL"
        result["errors"].append(f"File not found: {weather_file}")
        return result

    try:
        content = weather_file.read_text(encoding="utf-8")
    except Exception as exc:
        result["status"] = "FAIL"
        result["errors"].append(f"Unable to read file {weather_file}: {exc}")
        return result

    lines = content.splitlines()
    seen_ids = set()

    for line_num, line in enumerate(lines, start=1):
        line_str = line.strip()
        if not line_str:
            continue

        result["total_records"] += 1

        try:
            record = json.loads(line_str)
        except json.JSONDecodeError as exc:
            result["errors"].append(f"Line {line_num}: Malformed JSON - {exc}")
            continue

        if not isinstance(record, dict):
            result["errors"].append(f"Line {line_num}: Record is not a JSON object.")
            continue

        record_errors = []

        # Required fields check
        for field in REQUIRED_WEATHER_FIELDS:
            if field not in record or record[field] is None:
                record_errors.append(f"Missing required field '{field}'")

        # weather_record_id checks
        wid = record.get("weather_record_id")
        if wid is not None:
            if not isinstance(wid, str) or not wid.strip():
                record_errors.append("'weather_record_id' must be a non-empty string")
            else:
                if wid in seen_ids:
                    record_errors.append(f"Duplicate 'weather_record_id' detected: '{wid}'")
                    result["duplicate_weather_ids"].add(wid)
                seen_ids.add(wid)

        # Coordinate checks
        lat = record.get("latitude")
        lon = record.get("longitude")

        for name, val, min_val, max_val in [
            ("latitude", lat, -90.0, 90.0),
            ("longitude", lon, -180.0, 180.0),
        ]:
            if val is not None:
                if not is_number(val):
                    record_errors.append(f"'{name}' must be numeric (got {type(val).__name__})")
                elif not (min_val <= val <= max_val):
                    record_errors.append(f"'{name}' value {val} outside valid range [{min_val}, {max_val}]")

        # Timestamp check
        ts_str = record.get("forecast_time_utc")
        if ts_str is not None:
            if parse_iso8601(str(ts_str)) is None:
                record_errors.append(f"Invalid ISO 8601 timestamp in 'forecast_time_utc': '{ts_str}'")

        # Weather numeric metrics
        temp = record.get("temperature_2m_c")
        precip = record.get("precipitation_mm")
        wind = record.get("wind_speed_10m_kmh")
        vis = record.get("visibility_m")

        if temp is not None and not is_number(temp):
            record_errors.append(f"'temperature_2m_c' must be numeric (got {type(temp).__name__})")

        for name, val in [
            ("precipitation_mm", precip),
            ("wind_speed_10m_kmh", wind),
            ("visibility_m", vis),
        ]:
            if val is not None:
                if not is_number(val):
                    record_errors.append(f"'{name}' must be numeric (got {type(val).__name__})")
                elif val < 0:
                    record_errors.append(f"'{name}' cannot be negative (got {val})")

        if record_errors:
            for err in record_errors:
                result["errors"].append(f"Line {line_num}: {err}")
        else:
            result["valid_records"] += 1
            if wid:
                result["weather_record_ids"].add(wid)

    if result["total_records"] == 0:
        result["errors"].append("Weather file contains no records.")

    if result["errors"]:
        result["status"] = "FAIL"

    return result


# ---------------------------------------------------------------------------
# Shipment Validation
# ---------------------------------------------------------------------------

REQUIRED_SHIPMENT_FIELDS = [
    "shipment_id",
    "route_id",
    "carrier_id",
    "dispatch_time_utc",
    "planned_delivery_time_utc",
    "data_type",
]


def validate_shipments(shipments_file: Path, valid_route_ids: set[str] | None = None) -> dict:
    """Validate shipment dataset and check referential integrity against valid_route_ids.

    Returns dict with summary statistics, cross-dataset metrics, and errors.
    """
    result = {
        "status": "PASS",
        "total_records": 0,
        "valid_records": 0,
        "errors": [],
        "warnings": [],
        "shipment_ids": set(),
        "duplicate_shipment_ids": set(),
        "referenced_route_ids": set(),
        "nonexistent_route_shipments": 0,
        "invalid_timestamp_records": 0,
    }

    if not shipments_file.exists():
        result["status"] = "FAIL"
        result["errors"].append(f"File not found: {shipments_file}")
        return result

    try:
        content = shipments_file.read_text(encoding="utf-8")
    except Exception as exc:
        result["status"] = "FAIL"
        result["errors"].append(f"Unable to read file {shipments_file}: {exc}")
        return result

    lines = content.splitlines()
    seen_ids = set()

    for line_num, line in enumerate(lines, start=1):
        line_str = line.strip()
        if not line_str:
            continue

        result["total_records"] += 1

        try:
            record = json.loads(line_str)
        except json.JSONDecodeError as exc:
            result["errors"].append(f"Line {line_num}: Malformed JSON - {exc}")
            continue

        if not isinstance(record, dict):
            result["errors"].append(f"Line {line_num}: Record is not a JSON object.")
            continue

        record_errors = []

        # Required fields check
        for field in REQUIRED_SHIPMENT_FIELDS:
            if field not in record or record[field] is None:
                record_errors.append(f"Missing required field '{field}'")

        # shipment_id checks
        sid = record.get("shipment_id")
        if sid is not None:
            if not isinstance(sid, str) or not sid.strip():
                record_errors.append("'shipment_id' must be a non-empty string")
            else:
                if sid in seen_ids:
                    record_errors.append(f"Duplicate 'shipment_id' detected: '{sid}'")
                    result["duplicate_shipment_ids"].add(sid)
                seen_ids.add(sid)

        # carrier_id check
        carrier = record.get("carrier_id")
        if carrier is not None and (not isinstance(carrier, str) or not carrier.strip()):
            record_errors.append("'carrier_id' must be a non-empty string")

        # data_type check
        dtype = record.get("data_type")
        if dtype is not None and dtype != "synthetic":
            record_errors.append(f"'data_type' expected 'synthetic', got '{dtype}'")

        # route_id & referential integrity check
        rid = record.get("route_id")
        if rid is not None:
            if not isinstance(rid, str) or not rid.strip():
                record_errors.append("'route_id' must be a non-empty string")
            else:
                result["referenced_route_ids"].add(rid)
                if valid_route_ids is not None and rid not in valid_route_ids:
                    record_errors.append(
                        f"Referential integrity failure: route_id '{rid}' does not exist in routes dataset"
                    )
                    result["nonexistent_route_shipments"] += 1

        # Timestamps check
        dispatch_str = record.get("dispatch_time_utc")
        delivery_str = record.get("planned_delivery_time_utc")

        dispatch_dt = None
        delivery_dt = None

        if dispatch_str is not None:
            dispatch_dt = parse_iso8601(str(dispatch_str))
            if dispatch_dt is None:
                record_errors.append(f"Invalid ISO 8601 dispatch_time_utc: '{dispatch_str}'")

        if delivery_str is not None:
            delivery_dt = parse_iso8601(str(delivery_str))
            if delivery_dt is None:
                record_errors.append(f"Invalid ISO 8601 planned_delivery_time_utc: '{delivery_str}'")

        if (dispatch_str is not None and dispatch_dt is None) or (
            delivery_str is not None and delivery_dt is None
        ):
            result["invalid_timestamp_records"] += 1

        if dispatch_dt is not None and delivery_dt is not None:
            if delivery_dt <= dispatch_dt:
                record_errors.append(
                    f"Planned delivery time ({delivery_str}) must be strictly later than dispatch time ({dispatch_str})"
                )

        if record_errors:
            for err in record_errors:
                result["errors"].append(f"Line {line_num}: {err}")
        else:
            result["valid_records"] += 1
            if sid:
                result["shipment_ids"].add(sid)

    if result["total_records"] == 0:
        result["errors"].append("Shipment file contains no records.")

    if result["errors"]:
        result["status"] = "FAIL"

    return result


# ---------------------------------------------------------------------------
# Complete Dataset Validation Runner & Report
# ---------------------------------------------------------------------------

def validate_all_datasets(
    routes_file: Path = DEFAULT_ROUTES_FILE,
    weather_file: Path = DEFAULT_WEATHER_FILE,
    shipments_file: Path = DEFAULT_SHIPMENTS_FILE,
    synthetic_routes_file: Path | None = None,
) -> dict:
    """Run full validation suite on routes, weather, and shipments datasets.

    synthetic_routes_file (routes_synthetic.jsonl from generate_master_data) adds its route
    IDs to the set shipments are checked against; the file itself is validated in Silver.
    """
    # 1. Routes
    route_report = validate_routes(routes_file)

    # Valid route IDs set to check shipments against
    valid_route_ids = set(route_report["route_ids"])
    if synthetic_routes_file is not None and synthetic_routes_file.exists():
        with open(synthetic_routes_file, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rid = json.loads(line).get("route_id")
                    if rid:
                        valid_route_ids.add(rid)
    valid_route_ids = valid_route_ids or None

    # 2. Weather
    weather_report = validate_weather(weather_file)

    # 3. Shipments
    shipment_report = validate_shipments(shipments_file, valid_route_ids=valid_route_ids)

    # Overall Status
    overall_status = "PASS"
    if (
        route_report["status"] == "FAIL"
        or weather_report["status"] == "FAIL"
        or shipment_report["status"] == "FAIL"
    ):
        overall_status = "FAIL"

    return {
        "overall_status": overall_status,
        "routes": route_report,
        "weather": weather_report,
        "shipments": shipment_report,
    }


def print_report(results: dict, max_errors_to_print: int = 10) -> None:
    """Print clean terminal report distinguishing PASS, FAIL, WARNING."""
    print("=" * 70)
    print("DATASET VALIDATION REPORT")
    print("=" * 70)

    # Section 1: Route Dataset
    r = results["routes"]
    print(f"\n[1] Route Dataset: [{r['status']}]")
    print(f"    Total records: {r['total_records']}")
    print(f"    Valid records: {r['valid_records']}")
    print(f"    Unique route IDs: {len(r['route_ids'])}")
    if r["duplicate_route_ids"]:
        print(f"    WARNING/FAIL: Duplicate route IDs: {len(r['duplicate_route_ids'])}")
    if r["errors"]:
        print(f"    Errors ({len(r['errors'])}):")
        for err in r["errors"][:max_errors_to_print]:
            print(f"      - {err}")
        if len(r["errors"]) > max_errors_to_print:
            print(f"      ... and {len(r['errors']) - max_errors_to_print} more error(s)")

    # Section 2: Weather Dataset
    w = results["weather"]
    print(f"\n[2] Weather Dataset: [{w['status']}]")
    print(f"    Total records: {w['total_records']}")
    print(f"    Valid records: {w['valid_records']}")
    print(f"    Unique weather record IDs: {len(w['weather_record_ids'])}")
    if w["duplicate_weather_ids"]:
        print(f"    WARNING/FAIL: Duplicate weather record IDs: {len(w['duplicate_weather_ids'])}")
    if w["errors"]:
        print(f"    Errors ({len(w['errors'])}):")
        for err in w["errors"][:max_errors_to_print]:
            print(f"      - {err}")
        if len(w["errors"]) > max_errors_to_print:
            print(f"      ... and {len(w['errors']) - max_errors_to_print} more error(s)")

    # Section 3: Shipment Dataset
    s = results["shipments"]
    print(f"\n[3] Shipment Dataset: [{s['status']}]")
    print(f"    Total records: {s['total_records']}")
    print(f"    Valid records: {s['valid_records']}")
    print(f"    Unique shipment IDs: {len(s['shipment_ids'])}")
    print(f"    Unique route IDs referenced: {len(s['referenced_route_ids'])}")
    print(f"    Duplicate shipment IDs: {len(s['duplicate_shipment_ids'])}")
    print(f"    Invalid timestamp records: {s['invalid_timestamp_records']}")
    print(f"    Shipments referencing nonexistent routes: {s['nonexistent_route_shipments']}")
    if s["errors"]:
        print(f"    Errors ({len(s['errors'])}):")
        for err in s["errors"][:max_errors_to_print]:
            print(f"      - {err}")
        if len(s["errors"]) > max_errors_to_print:
            print(f"      ... and {len(s['errors']) - max_errors_to_print} more error(s)")

    # Section 4: Cross-Dataset Summary
    print("\n" + "-" * 70)
    print("CROSS-DATASET INTEGRITY SUMMARY")
    print("-" * 70)
    print(f"  Total Routes                : {r['total_records']}")
    print(f"  Total Weather Records       : {w['total_records']}")
    print(f"  Total Shipments             : {s['total_records']}")
    print(f"  Unique Shipment IDs         : {len(s['shipment_ids'])}")
    print(f"  Referenced Route IDs        : {len(s['referenced_route_ids'])}")
    print(f"  Nonexistent Route References: {s['nonexistent_route_shipments']}")
    print(f"  Duplicate Shipment IDs      : {len(s['duplicate_shipment_ids'])}")
    print(f"  Invalid Timestamp Records   : {s['invalid_timestamp_records']}")
    print(
        f"  Total Invalid Records       : {len(r['errors']) + len(w['errors']) + len(s['errors'])}"
    )

    print("\n" + "=" * 70)
    print(f"OVERALL VALIDATION STATUS: [{results['overall_status']}]")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate route, weather, and shipment datasets."
    )
    parser.add_argument(
        "--routes-file",
        type=Path,
        default=DEFAULT_ROUTES_FILE,
        help="Path to routes.jsonl",
    )
    parser.add_argument(
        "--weather-file",
        type=Path,
        default=DEFAULT_WEATHER_FILE,
        help="Path to weather.jsonl",
    )
    parser.add_argument(
        "--shipments-file",
        type=Path,
        default=DEFAULT_SHIPMENTS_FILE,
        help="Path to shipments.jsonl",
    )
    args = parser.parse_args()

    results = validate_all_datasets(
        routes_file=args.routes_file,
        weather_file=args.weather_file,
        shipments_file=args.shipments_file,
        synthetic_routes_file=DEFAULT_SYNTHETIC_ROUTES_FILE,
    )

    print_report(results)

    if results["overall_status"] == "FAIL":
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
