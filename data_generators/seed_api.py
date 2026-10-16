"""
seed_api.py – Fetch and cache OpenRouteService routes and Open-Meteo weather forecasts.

This module provides two main functions:
    fetch_route(origin, destination)   – driving-hgv route via OpenRouteService
    fetch_weather(latitude, longitude) – hourly weather forecast via Open-Meteo

Cached JSON responses are stored under data/route_cache/ and data/weather_cache/.
Repeated requests with the same parameters reuse the cache to avoid unnecessary
API calls.

Route cache entries are reused indefinitely for this prototype.
NOTE: In production, route estimates should be refreshed periodically because
road conditions, construction, and speed limits change over time.

Weather cache entries expire after a configurable TTL (default 6 hours).
"""

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Project root is two levels up from this file (data_generators/seed_api.py)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

ROUTE_CACHE_DIR = PROJECT_ROOT / "data" / "route_cache"
WEATHER_CACHE_DIR = PROJECT_ROOT / "data" / "weather_cache"

# OpenRouteService endpoint (free community instance hosted by HeiGIT)
ORS_BASE_URL = "https://api.heigit.org/openrouteservice/v2/directions/driving-hgv"

# Open-Meteo endpoint (free, no API key required)
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# HTTP request timeout in seconds
REQUEST_TIMEOUT = 30

# Weather cache time-to-live in seconds (default: 6 hours)
WEATHER_CACHE_TTL = int(os.environ.get("WEATHER_CACHE_TTL", 6 * 3600))

# Routing profile used for cache key generation
ROUTING_PROFILE = "driving-hgv"

# Default weather parameters
WEATHER_HOURLY_PARAMS = "temperature_2m,precipitation,wind_speed_10m,visibility"
WEATHER_FORECAST_DAYS = 1
WEATHER_TIMEZONE = "UTC"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _make_route_cache_key(origin: list, destination: list, profile: str) -> str:
    """Build a deterministic cache filename for a route request.

    The key is a SHA-256 hash of the origin, destination, and routing profile
    so that identical requests always map to the same file.

    Args:
        origin:      [longitude, latitude] of the start point.
        destination: [longitude, latitude] of the end point.
        profile:     Routing profile string, e.g. "driving-hgv".

    Returns:
        A hex-digest string used as the cache filename (without extension).
    """
    raw = json.dumps({
        "origin": origin,
        "destination": destination,
        "profile": profile,
    }, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def _make_weather_cache_key(
    latitude: float,
    longitude: float,
    hourly: str,
    forecast_days: int,
    timezone: str,
) -> str:
    """Build a deterministic cache filename for a weather request.

    Args:
        latitude:      Latitude of the location.
        longitude:     Longitude of the location.
        hourly:        Comma-separated hourly parameters.
        forecast_days: Number of forecast days.
        timezone:      Timezone string.

    Returns:
        A hex-digest string used as the cache filename (without extension).
    """
    raw = json.dumps({
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly,
        "forecast_days": forecast_days,
        "timezone": timezone,
    }, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def _read_cache(cache_dir: Path, cache_key: str, ttl: int | None = None) -> dict | None:
    """Read a cached JSON response if it exists and is still fresh.

    Args:
        cache_dir: Directory containing cache files.
        cache_key: The deterministic hash string (filename stem).
        ttl:       Maximum age in seconds.  None means no expiry.

    Returns:
        Parsed JSON dict if cache hit, otherwise None.
    """
    cache_file = cache_dir / f"{cache_key}.json"

    if not cache_file.exists():
        return None

    # Check freshness if a TTL is configured
    if ttl is not None:
        age = time.time() - cache_file.stat().st_mtime
        if age > ttl:
            print(f"  Cache expired ({age:.0f}s old, TTL={ttl}s): {cache_file.name}")
            return None

    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        print(f"  Cache hit: {cache_file.name}")
        return data
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  Warning: failed to read cache file {cache_file.name}: {exc}")
        return None


def _write_cache(cache_dir: Path, cache_key: str, data: dict) -> None:
    """Write a JSON response to the cache directory.

    Args:
        cache_dir: Directory to store the cache file.
        cache_key: The deterministic hash string (filename stem).
        data:      Parsed JSON dict to persist.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{cache_key}.json"
    cache_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Cached response: {cache_file.name}")


# ---------------------------------------------------------------------------
# Route fetching
# ---------------------------------------------------------------------------

def fetch_route(origin: list, destination: list) -> dict:
    """Fetch a driving-hgv route from OpenRouteService with local caching.

    The function sends a POST request to the ORS directions endpoint.
    If a cached response for the same origin/destination already exists,
    the cached version is returned immediately.

    Args:
        origin:      [longitude, latitude] of the start point.
        destination: [longitude, latitude] of the end point.

    Returns:
        The original ORS JSON response as a Python dict.

    Raises:
        SystemExit: If ORS_API_KEY is not set in the environment.
        requests.HTTPError: If the API returns an error status code.
        requests.RequestException: On network-level failures.
        ValueError: If the response does not contain expected route data.
    """
    # --- Read API key from environment ---
    api_key = os.environ.get("ORS_API_KEY")
    if not api_key:
        print("ERROR: ORS_API_KEY environment variable is not set.")
        print("Set it in PowerShell with:  $env:ORS_API_KEY = 'your-key-here'")
        sys.exit(1)

    # --- Check cache ---
    cache_key = _make_route_cache_key(origin, destination, ROUTING_PROFILE)
    cached = _read_cache(ROUTE_CACHE_DIR, cache_key, ttl=None)  # No expiry for routes
    if cached is not None:
        return cached

    # --- Build request ---
    print(f"  Requesting route: {origin} -> {destination}")
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }
    body = {
        "coordinates": [origin, destination],
    }

    # --- Send request ---
    try:
        response = requests.post(
            ORS_BASE_URL,
            headers=headers,
            json=body,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        print(f"  HTTP error from ORS: {exc.response.status_code} {exc.response.reason}")
        raise
    except requests.RequestException as exc:
        print(f"  Network error contacting ORS: {exc}")
        raise

    # --- Validate response ---
    data = response.json()
    if "routes" not in data or len(data["routes"]) == 0:
        raise ValueError("ORS response does not contain any routes.")

    # --- Cache and return ---
    _write_cache(ROUTE_CACHE_DIR, cache_key, data)
    return data


# ---------------------------------------------------------------------------
# Weather fetching
# ---------------------------------------------------------------------------

def fetch_weather(latitude: float, longitude: float) -> dict:
    """Fetch an hourly weather forecast from Open-Meteo with local caching.

    The cached response is reused if it is younger than WEATHER_CACHE_TTL
    (default 6 hours).  If the cache has expired, a fresh forecast is fetched.
    A failed API request will NOT overwrite a previously valid cache file.

    Args:
        latitude:  Latitude of the location.
        longitude: Longitude of the location.

    Returns:
        The original Open-Meteo JSON response as a Python dict.

    Raises:
        requests.HTTPError: If the API returns an error status code.
        requests.RequestException: On network-level failures.
        ValueError: If the response does not contain hourly data.
    """
    # --- Check cache ---
    cache_key = _make_weather_cache_key(
        latitude, longitude,
        WEATHER_HOURLY_PARAMS, WEATHER_FORECAST_DAYS, WEATHER_TIMEZONE,
    )
    cached = _read_cache(WEATHER_CACHE_DIR, cache_key, ttl=WEATHER_CACHE_TTL)
    if cached is not None:
        return cached

    # --- Build request ---
    print(f"  Requesting weather for ({latitude}, {longitude})")
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": WEATHER_HOURLY_PARAMS,
        "forecast_days": WEATHER_FORECAST_DAYS,
        "timezone": WEATHER_TIMEZONE,
    }

    # --- Send request ---
    try:
        response = requests.get(
            OPEN_METEO_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        print(f"  HTTP error from Open-Meteo: {exc.response.status_code} {exc.response.reason}")
        raise
    except requests.RequestException as exc:
        print(f"  Network error contacting Open-Meteo: {exc}")
        raise

    # --- Validate response ---
    data = response.json()
    if "hourly" not in data:
        raise ValueError("Open-Meteo response does not contain hourly data.")

    # --- Cache and return ---
    _write_cache(WEATHER_CACHE_DIR, cache_key, data)
    return data


# ---------------------------------------------------------------------------
# Main – local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Example coordinates (Heidelberg area, Germany)
    example_origin = [8.681495, 49.414610]
    example_destination = [8.687872, 49.420318]

    print("=" * 60)
    print("seed_api.py – Local API Cache Test")
    print("=" * 60)

    # --- Route ---
    print("\n[1] Fetching route...")
    try:
        route_data = fetch_route(example_origin, example_destination)
        route = route_data["routes"][0]
        summary = route.get("summary", {})
        distance_km = summary.get("distance", 0) / 1000
        duration_min = summary.get("duration", 0) / 60
        print(f"  Route distance : {distance_km:.2f} km")
        print(f"  Route duration : {duration_min:.1f} min")
    except Exception as exc:
        print(f"  Route fetch failed: {exc}")

    # --- Weather ---
    # Use latitude/longitude near the route midpoint
    weather_lat = (example_origin[1] + example_destination[1]) / 2
    weather_lon = (example_origin[0] + example_destination[0]) / 2

    print(f"\n[2] Fetching weather for ({weather_lat:.4f}, {weather_lon:.4f})...")
    try:
        weather_data = fetch_weather(weather_lat, weather_lon)
        hourly = weather_data.get("hourly", {})
        num_records = len(hourly.get("time", []))
        location_lat = weather_data.get("latitude", "?")
        location_lon = weather_data.get("longitude", "?")
        print(f"  Weather location  : ({location_lat}, {location_lon})")
        print(f"  Hourly records    : {num_records}")
    except Exception as exc:
        print(f"  Weather fetch failed: {exc}")

    print("\n" + "=" * 60)
    print("Done.")
