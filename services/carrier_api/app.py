"""
app.py – Mock carrier tracking API: serves the generated shipments the way a carrier's REST API would.

Stands in for a real carrier API (DHL, FedEx, ...), which needs a commercial account. The pipeline's
API client talks to it exactly as it would to a real one, so switching to a real carrier later
changes only the base URL and the key.

    GET /health                                   no key needed
    GET /v1/shipments?updated_since=&limit=&cursor=   header X-API-Key

Shipments are served oldest change first (updated_at, then shipment_id), only those changed after
updated_since, in pages of `limit` (1-1000). next_cursor is null on the last page. A cursor belongs
to the query it came from: reusing it with another updated_since is a 400.

Like a real API it fails now and then: CHAOS_429_RATE of requests get 429 with a Retry-After header,
CHAOS_500_RATE get 500. Bad shipments (the generator's ~2% corruptions) are served as they are; the
Silver quality gate deals with them, as it would with a real carrier's data.

Settings (environment variables, read when the app is created; create_app() arguments override them):
    CARRIER_API_KEY      the key clients must send (required: the app does not start without one)
    CARRIER_DATA_FILE    default data/shipments/shipments.jsonl (written by generate_shipments)
    CHAOS_429_RATE       default 0.05
    CHAOS_500_RATE       default 0.03
    RETRY_AFTER_SECONDS  default 2

Run (data is read once at start; restart after regenerating it):
    $env:CARRIER_API_KEY = "local-dev-key"
    uvicorn services.carrier_api.app:create_app --factory --port 8080
    # http://localhost:8080/docs  - Swagger UI: Authorize with the key, then "Try it out"
"""

import base64
import binascii
import bisect
import json
import os
import random
import secrets
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_FILE = ROOT / "data" / "shipments" / "shipments.jsonl"
MAX_LIMIT = 1000


class ApiError(Exception):
    """An HTTP error with a JSON body {"error": ..., "detail": ...}."""

    def __init__(self, status: int, error: str, detail: str, headers: Optional[dict] = None):
        self.status, self.error, self.detail, self.headers = status, error, detail, headers or {}


def parse_utc(text: str) -> datetime:
    """ISO 8601 with a time zone ('...Z' or '+01:00'); a time without a zone is ambiguous, so rejected."""
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("time zone missing")
    return value


def load_shipments(path: Path) -> List[dict]:
    """Shipments from the JSONL file, oldest change first; ties broken by shipment_id."""
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return sorted(rows, key=lambda r: (parse_utc(r["updated_at"]), str(r.get("shipment_id") or "")))


def encode_cursor(since: Optional[str], offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"since": since, "offset": offset}).encode()).decode()


def decode_cursor(cursor: str) -> dict:
    try:
        state = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        if not isinstance(state, dict) or not isinstance(state.get("offset"), int) or state["offset"] < 0:
            raise ValueError
        return state
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise ApiError(400, "invalid_cursor", "cursor is not one this API issued") from None


def create_app(
    data_file: Optional[Path] = None,
    api_key: Optional[str] = None,
    chaos_429_rate: Optional[float] = None,
    chaos_500_rate: Optional[float] = None,
    retry_after_seconds: Optional[int] = None,
    rng: Optional[random.Random] = None,
) -> FastAPI:
    api_key = api_key or os.environ.get("CARRIER_API_KEY")
    if not api_key:
        raise RuntimeError("Set CARRIER_API_KEY: the mock API never runs without authentication.")
    data_file = Path(data_file or os.environ.get("CARRIER_DATA_FILE") or DEFAULT_DATA_FILE)
    rate_429 = float(os.environ.get("CHAOS_429_RATE", 0.05)) if chaos_429_rate is None else chaos_429_rate
    rate_500 = float(os.environ.get("CHAOS_500_RATE", 0.03)) if chaos_500_rate is None else chaos_500_rate
    retry_after = int(os.environ.get("RETRY_AFTER_SECONDS", 2)) if retry_after_seconds is None else retry_after_seconds
    rng = rng or random.Random()

    shipments = load_shipments(data_file)
    changed_at = [parse_utc(r["updated_at"]) for r in shipments]      # sorted, for bisect

    app = FastAPI(
        title="Mock Carrier Tracking API",
        description="Stands in for a carrier's shipment tracking API (see services/carrier_api/README.md).",
        version="1.0",
    )
    key_header = APIKeyHeader(name="X-API-Key", auto_error=False)   # also adds "Authorize" to /docs

    @app.exception_handler(ApiError)
    async def api_error(_: Request, exc: ApiError):
        return JSONResponse({"error": exc.error, "detail": exc.detail}, status_code=exc.status, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def bad_request(_: Request, exc: RequestValidationError):
        detail = "; ".join(f"{'.'.join(map(str, e['loc'][1:]))}: {e['msg']}" for e in exc.errors())
        return JSONResponse({"error": "bad_request", "detail": detail}, status_code=400)

    @app.get("/health")
    def health():
        return {"status": "ok", "shipments": len(shipments)}

    @app.get("/v1/shipments")
    def list_shipments(updated_since: Optional[str] = None, limit: int = MAX_LIMIT, cursor: Optional[str] = None,
                       sent_key: Optional[str] = Security(key_header)):
        # 1. Who is asking (before anything else, as a real API would)
        if not sent_key or not secrets.compare_digest(sent_key, api_key):
            raise ApiError(401, "unauthorized", "missing or wrong X-API-Key")
        # 2. The failures a real API has now and then
        if rng.random() < rate_429:
            raise ApiError(429, "rate_limited", f"too many requests, retry after {retry_after} s",
                           headers={"Retry-After": str(retry_after)})
        if rng.random() < rate_500:
            raise ApiError(500, "internal_error", "temporary server error, retry later")
        # 3. The request itself
        if not 1 <= limit <= MAX_LIMIT:
            raise ApiError(400, "bad_request", f"limit must be between 1 and {MAX_LIMIT}")
        start = 0
        if updated_since is not None:
            try:
                start = bisect.bisect_right(changed_at, parse_utc(updated_since))   # strictly after
            except ValueError:
                raise ApiError(400, "bad_request", "updated_since must be ISO 8601 with a time zone, "
                                                    "e.g. 2026-11-01T00:00:00Z") from None
        if cursor is not None:
            state = decode_cursor(cursor)
            if state.get("since") != updated_since:
                raise ApiError(400, "invalid_cursor", "cursor belongs to a request with another updated_since")
            start = state["offset"]
        page = shipments[start:start + limit]
        end = start + len(page)
        return {
            "data": page,
            "count": len(page),
            "next_cursor": encode_cursor(updated_since, end) if end < len(shipments) else None,
        }

    return app
