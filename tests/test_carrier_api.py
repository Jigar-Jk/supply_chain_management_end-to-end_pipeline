"""
test_carrier_api.py – The mock carrier tracking API (services/carrier_api/app.py) behaves like a real one.

Runs in-process with FastAPI's TestClient: no server, no port, no network. The data is a small
hand-built file: 2,500 shipments with some equal updated_at values, so ties must not cause a
shipment to be skipped or served twice across a page boundary.

Run locally with:
    python -m unittest tests.test_carrier_api -v
"""

import json
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from fastapi.testclient import TestClient
    from services.carrier_api.app import MAX_LIMIT, create_app
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from scm_platform.ingest_bronze import DATASET_SCHEMAS
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

KEY = "test-key"
HEADERS = {"X-API-Key": KEY}
START = datetime(2026, 11, 1, tzinfo=timezone.utc)
COUNT = 2_500


def iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def shipment(i: int) -> dict:
    """Every 3 shipments share one updated_at (ties); written out of order on purpose."""
    return {
        "shipment_id": f"SHP-{i:05d}", "order_id": f"ORD-{i:05d}", "warehouse_id": "WH-0001",
        "route_id": "r1", "carrier_id": "CARRIER_DHL", "dispatch_time_utc": iso(START),
        "planned_delivery_time_utc": iso(START + timedelta(hours=10)), "actual_delivery_time_utc": None,
        "shipment_status": "IN_TRANSIT", "ordered_quantity": 10, "shipped_quantity": 10,
        "weather_risk": "LOW", "data_type": "synthetic", "updated_at": iso(START + timedelta(minutes=i // 3)),
    }


@unittest.skipUnless(FASTAPI_AVAILABLE, "fastapi and httpx are required (requirements-dev.txt)")
class TestCarrierApi(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.data_file = Path(cls.tmp.name) / "shipments.jsonl"
        rows = [shipment(i) for i in range(COUNT)]
        random.Random(7).shuffle(rows)
        cls.data_file.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        cls.client = cls.make_client()

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    @classmethod
    def make_client(cls, **overrides):
        settings = dict(data_file=cls.data_file, api_key=KEY, chaos_429_rate=0.0, chaos_500_rate=0.0)
        return TestClient(create_app(**{**settings, **overrides}))

    def get(self, client=None, headers=HEADERS, **params):
        return (client or self.client).get("/v1/shipments", params=params, headers=headers)

    def all_pages(self, **params):
        pages, cursor = [], None
        while True:
            body = self.get(**params, **({"cursor": cursor} if cursor else {})).json()
            pages.append(body)
            cursor = body["next_cursor"]
            if cursor is None:
                return pages

    # ------------------------------------------------------------------ basics

    def test_health_needs_no_key(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "shipments": COUNT})

    def test_missing_or_wrong_key_is_401(self):
        for headers in ({}, {"X-API-Key": "wrong"}):
            response = self.get(headers=headers)
            self.assertEqual(response.status_code, 401, headers)
            self.assertEqual(response.json()["error"], "unauthorized")

    def test_the_app_refuses_to_start_without_a_key(self):
        with self.assertRaises(RuntimeError):
            create_app(data_file=self.data_file, api_key="")

    # ------------------------------------------------------------------ pagination

    def test_first_page_is_full_and_has_a_cursor(self):
        body = self.get().json()
        self.assertEqual(body["count"], MAX_LIMIT)
        self.assertEqual(len(body["data"]), MAX_LIMIT)
        self.assertIsNotNone(body["next_cursor"])

    def test_following_cursors_returns_every_shipment_once_in_change_order(self):
        pages = self.all_pages(limit=700)                      # 700 does not divide 2,500 or the 3-way ties
        self.assertEqual([p["count"] for p in pages], [700, 700, 700, 400])
        served = [s for p in pages for s in p["data"]]
        ids = [s["shipment_id"] for s in served]
        self.assertEqual(len(ids), COUNT)
        self.assertEqual(len(set(ids)), COUNT, "a shipment was served twice")
        keys = [(s["updated_at"], s["shipment_id"]) for s in served]
        self.assertEqual(keys, sorted(keys), "not oldest change first")

    def test_shipments_are_served_unchanged(self):
        """The API passes the carrier's records through as they are; Bronze keeps the same contract."""
        stored = {r["shipment_id"]: r for r in map(json.loads, self.data_file.read_text().splitlines())}
        for s in self.get(limit=50).json()["data"]:
            self.assertEqual(s, stored[s["shipment_id"]])

    @unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required for the Bronze schema")
    def test_fields_match_the_bronze_shipments_contract(self):
        contract = {f.name for f in DATASET_SCHEMAS["shipments"]().fields}
        for s in self.get(limit=10).json()["data"]:
            self.assertLessEqual(set(s), contract)

    # ------------------------------------------------------------------ incremental

    def test_updated_since_returns_only_later_changes(self):
        since = START + timedelta(minutes=500)                 # shipments 1500.. changed after this
        served = [s for p in self.all_pages(updated_since=iso(since)) for s in p["data"]]
        self.assertEqual(len(served), COUNT - 1503)            # 1500-1502 share minute 500: not after it
        self.assertTrue(all(datetime.fromisoformat(s["updated_at"].replace("Z", "+00:00")) > since for s in served))

    def test_updated_since_accepts_any_time_zone(self):
        utc = self.get(updated_since="2026-11-01T08:20:00Z", limit=5).json()["data"]
        ist = self.get(updated_since="2026-11-01T13:50:00+05:30", limit=5).json()["data"]
        self.assertEqual(utc, ist)

    def test_updated_since_after_the_newest_change_is_an_empty_last_page(self):
        body = self.get(updated_since="2027-01-01T00:00:00Z").json()
        self.assertEqual((body["count"], body["data"], body["next_cursor"]), (0, [], None))

    # ------------------------------------------------------------------ bad requests

    def test_bad_requests_are_400(self):
        cases = [
            {"updated_since": "yesterday"},
            {"updated_since": "2026-11-01T00:00:00"},            # no time zone: ambiguous
            {"limit": 0}, {"limit": MAX_LIMIT + 1}, {"limit": "many"},
            {"cursor": "not-a-cursor"},
        ]
        for params in cases:
            response = self.get(**params)
            self.assertEqual(response.status_code, 400, params)
            self.assertIn(response.json()["error"], ("bad_request", "invalid_cursor"), params)

    def test_a_cursor_only_works_with_its_own_updated_since(self):
        cursor = self.get(updated_since="2026-11-01T01:00:00Z").json()["next_cursor"]
        response = self.get(updated_since="2026-11-01T05:00:00Z", cursor=cursor)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_cursor")

    # ------------------------------------------------------------------ failures a real API has

    def test_rate_limit_is_429_with_retry_after(self):
        client = self.make_client(chaos_429_rate=1.0, retry_after_seconds=3)
        response = self.get(client=client)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["Retry-After"], "3")
        self.assertEqual(response.json()["error"], "rate_limited")

    def test_server_error_is_500(self):
        response = self.get(client=self.make_client(chaos_500_rate=1.0))
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["error"], "internal_error")

    def test_authentication_comes_before_the_random_failures(self):
        """A caller with a wrong key always learns it, instead of retrying a random 429 or 500."""
        client = self.make_client(chaos_429_rate=1.0, chaos_500_rate=1.0)
        self.assertEqual(self.get(client=client, headers={"X-API-Key": "wrong"}).status_code, 401)


if __name__ == "__main__":
    unittest.main()
