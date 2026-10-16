"""
normalize_api_data.py – Transform cached API responses into clean JSONL datasets.

Reads OpenRouteService and Open-Meteo JSON cache files, extracts relevant
fields, and writes normalized records as JSONL to data/routes/ and data/weather/.

This module does NOT make any API or network calls.
It operates exclusively on locally cached JSON files.

Usage:
    python -m data_generators.normalize_api_data
"""

import hashlib
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ROUTE_CACHE_DIR = PROJECT_ROOT / "data" / "route_cache"
WEATHER_CACHE_DIR = PROJECT_ROOT / "data" / "weather_cache"

ROUTES_OUTPUT_DIR = PROJECT_ROOT / "data" / "routes"
WEATHER_OUTPUT_DIR = PROJECT_ROOT / "data" / "weather"

ROUTES_OUTPUT_FILE = ROUTES_OUTPUT_DIR / "routes.jsonl"
WEATHER_OUTPUT_FILE = WEATHER_OUTPUT_DIR / "weather.jsonl"


# ---------------------------------------------------------------------------
# Route normalization
# ---------------------------------------------------------------------------

def normalize_route(data: dict, source_file: str) -> dict | None:
    """Extract a normalized route record from an ORS API response.

    The function reads the first route's summary for distance/duration, and
    recovers origin/destination coordinates from metadata.query.coordinates
    which the ORS API echoes back in every response.

    Args:
        data:        Parsed JSON dict from an ORS cache file.
        source_file: Name of the source cache file (for diagnostics).

    Returns:
        A normalized route dict, or None if required fields are missing.

    Raises:
        ValueError: If the response structure is invalid or missing
                    required fields.
    """
    # --- Validate top-level structure ---
    routes = data.get("routes")
    if not routes or not isinstance(routes, list) or len(routes) == 0:
        raise ValueError("Missing or empty 'routes' array.")

    route = routes[0]
    summary = route.get("summary")
    if not summary or not isinstance(summary, dict):
        raise ValueError("Missing 'routes[0].summary'.")

    distance = summary.get("distance")
    duration = summary.get("duration")
    if distance is None or duration is None:
        raise ValueError("Missing 'distance' or 'duration' in route summary.")

    # --- Extract coordinates from metadata.query.coordinates ---
    metadata = data.get("metadata", {})
    query = metadata.get("query", {})
    coordinates = query.get("coordinates")
    profile = query.get("profile", "driving-hgv")

    if not coordinates or len(coordinates) < 2:
        raise ValueError(
            "Cannot recover origin/destination coordinates: "
            "'metadata.query.coordinates' is missing or incomplete. "
            "The ORS response did not echo back the request coordinates."
        )

    origin = coordinates[0]   # [longitude, latitude]
    destination = coordinates[1]

    if len(origin) < 2 or len(destination) < 2:
        raise ValueError("Coordinate arrays must have at least 2 elements [lon, lat].")

    # --- Build deterministic route_id ---
    # Hash based on origin, destination, and profile so the same route
    # always produces the same ID (avoids duplicates on re-runs).
    id_input = json.dumps({
        "origin": origin,
        "destination": destination,
        "profile": profile,
    }, sort_keys=True)
    route_id = hashlib.sha256(id_input.encode()).hexdigest()[:16]

    return {
        "route_id": route_id,
        "origin_longitude": origin[0],
        "origin_latitude": origin[1],
        "destination_longitude": destination[0],
        "destination_latitude": destination[1],
        "distance_meters": distance,
        "duration_seconds": duration,
        "routing_profile": profile,
        "source": "openrouteservice",
    }


def process_route_cache(
    cache_dir: Path,
    output_file: Path,
) -> dict:
    """Read all route cache files and write a normalized JSONL file.

    Args:
        cache_dir:   Path to the route cache directory.
        output_file: Path to write the routes.jsonl output.

    Returns:
        A summary dict with counts of discovered, normalized, and skipped files.
    """
    summary = {
        "discovered": 0,
        "normalized": 0,
        "skipped": [],
    }

    if not cache_dir.exists():
        print(f"  Route cache directory not found: {cache_dir}")
        return summary

    cache_files = sorted(cache_dir.glob("*.json"))
    summary["discovered"] = len(cache_files)

    # Collect all normalized records, keyed by route_id to avoid duplicates
    records: dict[str, dict] = {}

    for cache_file in cache_files:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            reason = f"Failed to read/parse JSON: {exc}"
            summary["skipped"].append((cache_file.name, reason))
            print(f"  SKIP {cache_file.name}: {reason}")
            continue

        try:
            record = normalize_route(data, cache_file.name)
        except ValueError as exc:
            reason = str(exc)
            summary["skipped"].append((cache_file.name, reason))
            print(f"  SKIP {cache_file.name}: {reason}")
            continue

        if record is None:
            reason = "normalize_route returned None"
            summary["skipped"].append((cache_file.name, reason))
            print(f"  SKIP {cache_file.name}: {reason}")
            continue

        records[record["route_id"]] = record

    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for record in records.values():
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary["normalized"] = len(records)
    return summary


# ---------------------------------------------------------------------------
# Weather normalization
# ---------------------------------------------------------------------------

def normalize_weather(data: dict, source_file: str) -> list[dict]:
    """Extract normalized hourly weather records from an Open-Meteo response.

    Creates one record per hourly timestamp by aligning the parallel arrays
    in the response's hourly block.

    Args:
        data:        Parsed JSON dict from an Open-Meteo cache file.
        source_file: Name of the source cache file (for diagnostics).

    Returns:
        A list of normalized weather record dicts.

    Raises:
        ValueError: If the response structure is invalid or arrays are
                    mismatched.
    """
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    if latitude is None or longitude is None:
        raise ValueError("Missing 'latitude' or 'longitude' in response.")

    hourly = data.get("hourly")
    if not hourly or not isinstance(hourly, dict):
        raise ValueError("Missing or invalid 'hourly' block.")

    # --- Extract required arrays ---
    required_keys = ["time", "temperature_2m", "precipitation",
                     "wind_speed_10m", "visibility"]
    arrays = {}
    for key in required_keys:
        arr = hourly.get(key)
        if arr is None:
            raise ValueError(f"Missing required hourly field: '{key}'.")
        if not isinstance(arr, list):
            raise ValueError(f"Hourly field '{key}' is not a list.")
        arrays[key] = arr

    # --- Verify consistent array lengths ---
    lengths = {key: len(arr) for key, arr in arrays.items()}
    unique_lengths = set(lengths.values())
    if len(unique_lengths) != 1:
        raise ValueError(
            f"Mismatched hourly array lengths: {lengths}. "
            "All arrays must have the same number of elements."
        )

    num_records = lengths["time"]
    records = []

    for i in range(num_records):
        forecast_time = arrays["time"][i]

        # Build deterministic weather_record_id from location + timestamp
        id_input = f"{latitude}:{longitude}:{forecast_time}"
        weather_record_id = hashlib.sha256(id_input.encode()).hexdigest()[:16]

        records.append({
            "weather_record_id": weather_record_id,
            "latitude": latitude,
            "longitude": longitude,
            "forecast_time_utc": forecast_time,
            "temperature_2m_c": arrays["temperature_2m"][i],
            "precipitation_mm": arrays["precipitation"][i],
            "wind_speed_10m_kmh": arrays["wind_speed_10m"][i],
            "visibility_m": arrays["visibility"][i],
            "source": "open-meteo",
        })

    return records


def process_weather_cache(
    cache_dir: Path,
    output_file: Path,
) -> dict:
    """Read all weather cache files and write a normalized JSONL file.

    Args:
        cache_dir:   Path to the weather cache directory.
        output_file: Path to write the weather.jsonl output.

    Returns:
        A summary dict with counts of discovered, normalized, and skipped files.
    """
    summary = {
        "discovered": 0,
        "normalized": 0,
        "skipped": [],
    }

    if not cache_dir.exists():
        print(f"  Weather cache directory not found: {cache_dir}")
        return summary

    cache_files = sorted(cache_dir.glob("*.json"))
    summary["discovered"] = len(cache_files)

    # Collect all records, keyed by weather_record_id to avoid duplicates
    records: dict[str, dict] = {}

    for cache_file in cache_files:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            reason = f"Failed to read/parse JSON: {exc}"
            summary["skipped"].append((cache_file.name, reason))
            print(f"  SKIP {cache_file.name}: {reason}")
            continue

        try:
            hourly_records = normalize_weather(data, cache_file.name)
        except ValueError as exc:
            reason = str(exc)
            summary["skipped"].append((cache_file.name, reason))
            print(f"  SKIP {cache_file.name}: {reason}")
            continue

        for record in hourly_records:
            records[record["weather_record_id"]] = record

    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        for record in records.values():
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    summary["normalized"] = len(records)
    return summary


# ---------------------------------------------------------------------------
# Main – run normalization pipeline
# ---------------------------------------------------------------------------

def run(
    route_cache_dir: Path = ROUTE_CACHE_DIR,
    weather_cache_dir: Path = WEATHER_CACHE_DIR,
    routes_output: Path = ROUTES_OUTPUT_FILE,
    weather_output: Path = WEATHER_OUTPUT_FILE,
) -> None:
    """Run the full normalization pipeline and print a summary."""

    print("=" * 60)
    print("normalize_api_data.py – Data Normalization Pipeline")
    print("=" * 60)

    # --- Routes ---
    print("\n[1] Processing route cache...")
    route_summary = process_route_cache(route_cache_dir, routes_output)

    # --- Weather ---
    print("\n[2] Processing weather cache...")
    weather_summary = process_weather_cache(weather_cache_dir, weather_output)

    # --- Summary ---
    print("\n" + "-" * 60)
    print("SUMMARY")
    print("-" * 60)

    print("\nRoutes:")
    print(f"  Cache files discovered : {route_summary['discovered']}")
    print(f"  Records normalized     : {route_summary['normalized']}")
    if route_summary["skipped"]:
        print(f"  Files skipped          : {len(route_summary['skipped'])}")
        for filename, reason in route_summary["skipped"]:
            print(f"    - {filename}: {reason}")
    else:
        print("  Files skipped          : 0")
    print(f"  Output file            : {routes_output}")

    print("\nWeather:")
    print(f"  Cache files discovered : {weather_summary['discovered']}")
    print(f"  Records normalized     : {weather_summary['normalized']}")
    if weather_summary["skipped"]:
        print(f"  Files skipped          : {len(weather_summary['skipped'])}")
        for filename, reason in weather_summary["skipped"]:
            print(f"    - {filename}: {reason}")
    else:
        print("  Files skipped          : 0")
    print(f"  Output file            : {weather_output}")

    print("\n" + "=" * 60)
    print("Done.")


if __name__ == "__main__":
    run()
