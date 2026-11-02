"""
test_ingest_api.py – The carrier API ingestion end to end: mock API -> client -> landing files + watermark.

Everything real except the network: the mock carrier API runs in-process (TestClient is the HTTP
session), the shared client retries as in production, the watermark lives in a temp JSON file and
the landing zone is a temp folder. The Databricks writer is checked against a fake dbutils.

Run locally with:
    python -m unittest tests.test_ingest_api -v
"""

import json
import logging
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

try:
    from fastapi.testclient import TestClient
    from services.carrier_api.app import create_app
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

from scm_platform.api.carrier import SOURCE, fetch_shipments, safe_run_id
from scm_platform.api.client import ApiClient, ApiError
from scm_platform.api.landing import DbutilsLanding, LocalLanding
from scm_platform.api.watermark import FileWatermarkStore
from scm_platform import ingest_api

KEY = "test-key-987"
START = datetime(2026, 11, 1, tzinfo=timezone.utc)


def iso(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def shipment(i):
    return {"shipment_id": f"SHP-{i:05d}", "order_id": f"ORD-{i:05d}", "warehouse_id": "WH-0001",
            "route_id": "r1", "carrier_id": "CARRIER_DHL", "dispatch_time_utc": iso(START),
            "planned_delivery_time_utc": iso(START + timedelta(hours=10)), "actual_delivery_time_utc": None,
            "shipment_status": "IN_TRANSIT", "ordered_quantity": 10, "shipped_quantity": 10,
            "weather_risk": "LOW", "data_type": "synthetic", "updated_at": iso(START + timedelta(minutes=i))}


class Session:
    """The in-process mock API as the client's HTTP session; drops `timeout`, which TestClient ignores."""

    def __init__(self, app):
        self.client = TestClient(app)

    def get(self, url, params=None, headers=None, timeout=None):
        return self.client.get(url, params=params, headers=headers)


class FailOnCall:
    """Wraps a session: the n-th GET (1-based) and every later one answer 500."""

    def __init__(self, session, from_call):
        self.session, self.from_call, self.calls = session, from_call, 0

    def get(self, *args, **kwargs):
        self.calls += 1
        if self.calls >= self.from_call:
            return mock.Mock(status_code=500, headers={}, text="down")
        return self.session.get(*args, **kwargs)


@unittest.skipUnless(FASTAPI_AVAILABLE, "fastapi and httpx are required (requirements-dev.txt)")
class TestCarrierIngestion(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.data_file, self.landing_dir = root / "carrier.jsonl", root / "landing" / "shipments"
        self.store = FileWatermarkStore(root / "state.json")
        self.write_carrier_data(range(2_500))

    def tearDown(self):
        self.tmp.cleanup()

    def write_carrier_data(self, ids):
        self.data_file.write_text("".join(json.dumps(shipment(i)) + "\n" for i in ids), encoding="utf-8")

    def client(self, session=None):
        session = session or Session(create_app(data_file=self.data_file, api_key=KEY,
                                                   chaos_429_rate=0.0, chaos_500_rate=0.0))
        return ApiClient("http://testserver", api_key=KEY, session=session, sleep=lambda s: None)

    def fetch(self, run_id, client=None, **kwargs):
        return fetch_shipments(client or self.client(), self.store, LocalLanding(self.landing_dir), run_id,
                               page_size=1000, initial_days=3650, **kwargs)

    def landed(self):
        return [json.loads(line) for f in sorted(self.landing_dir.glob("*.jsonl"))
                for line in f.read_text(encoding="utf-8").splitlines()]

    # ------------------------------------------------------------------ the normal runs

    def test_first_run_lands_everything_and_sets_the_watermark(self):
        result = self.fetch("run-1")
        self.assertEqual((result.rows, result.pages, len(result.files)), (2_500, 3, 3))
        self.assertEqual([Path(f).name for f in result.files],
                         ["api_run-1_p0001.jsonl", "api_run-1_p0002.jsonl", "api_run-1_p0003.jsonl"])
        self.assertEqual(len({s["shipment_id"] for s in self.landed()}), 2_500)
        newest = iso(START + timedelta(minutes=2_499))
        self.assertEqual((result.watermark_after, self.store.read(SOURCE)), (newest, newest))

    def test_landed_records_are_the_api_records_unchanged(self):
        self.fetch("run-1")
        expected = {s["shipment_id"]: s for s in map(json.loads, self.data_file.read_text().splitlines())}
        for record in self.landed():
            self.assertEqual(record, expected[record["shipment_id"]])

    def test_a_run_with_nothing_new_writes_no_file_and_keeps_the_watermark(self):
        first = self.fetch("run-1")
        second = self.fetch("run-2")
        self.assertEqual((second.rows, second.files), (0, []))
        self.assertEqual(second.watermark_after, first.watermark_after)
        self.assertEqual(len(list(self.landing_dir.glob("*.jsonl"))), 3)

    def test_the_next_run_fetches_only_what_changed_since(self):
        self.fetch("run-1")
        self.write_carrier_data(range(2_600))                 # the carrier recorded 100 more changes
        result = self.fetch("run-2")
        self.assertEqual(result.rows, 100)
        ids = sorted(s["shipment_id"] for f in result.files for s in map(json.loads, Path(f).read_text().splitlines()))
        self.assertEqual(ids[0], "SHP-02500")
        self.assertEqual(result.watermark_after, iso(START + timedelta(minutes=2_599)))

    # ------------------------------------------------------------------ failures

    def test_a_failure_mid_way_keeps_the_watermark_so_the_next_run_resends(self):
        api = Session(create_app(data_file=self.data_file, api_key=KEY, chaos_429_rate=0.0, chaos_500_rate=0.0))
        failing = ApiClient("http://testserver", api_key=KEY, session=FailOnCall(api, from_call=2),
                            max_retries=2, sleep=lambda s: None)
        with self.assertRaises(ApiError):
            self.fetch("run-1", client=failing)
        self.assertIsNone(self.store.read(SOURCE), "watermark moved although pages were missing")
        self.assertEqual(len(self.landed()), 1_000)           # page 1 landed before the failure

        retry = self.fetch("run-2")                            # the API is back: everything again
        self.assertEqual(retry.rows, 2_500)
        self.assertEqual(len(self.landed()), 3_500)            # page 1 twice: Silver keeps one version
        self.assertEqual(len({s["shipment_id"] for s in self.landed()}), 2_500)

    def test_random_429_and_500_are_absorbed(self):
        api = Session(create_app(data_file=self.data_file, api_key=KEY, chaos_429_rate=0.3, chaos_500_rate=0.2,
                                    retry_after_seconds=0))
        result = self.fetch("run-1", client=ApiClient("http://testserver", api_key=KEY, session=api,
                                                      max_retries=10, sleep=lambda s: None))
        self.assertEqual(result.rows, 2_500)

    def test_a_wrong_key_fails_at_once_and_lands_nothing(self):
        api = Session(create_app(data_file=self.data_file, api_key="the-real-key", chaos_429_rate=0.0,
                                    chaos_500_rate=0.0))
        with self.assertRaises(ApiError) as caught:
            self.fetch("run-1", client=self.client(session=api))
        self.assertEqual(caught.exception.status, 401)
        self.assertFalse(self.landing_dir.exists() and any(self.landing_dir.iterdir()))

    # ------------------------------------------------------------------ entry point

    def test_entry_point_reads_the_key_from_the_environment_and_never_logs_it(self):
        api = Session(create_app(data_file=self.data_file, api_key=KEY, chaos_429_rate=0.0, chaos_500_rate=0.0))
        args = ingest_api.parse_args(["--source", "carrier", "--base-url", "http://testserver",
                                      "--landing", str(self.landing_dir), "--state-file", str(Path(self.tmp.name) / "s.json"),
                                      "--run-id", "job:42", "--initial-days", "3650"])
        with mock.patch.dict(os.environ, {"CARRIER_API_KEY": KEY}), \
                self.assertLogs("scm_platform.api", logging.INFO) as logs:
            result = ingest_api.run(args, session=api)
        self.assertEqual(result.rows, 2_500)
        self.assertTrue(all("api_job_42_p" in f for f in result.files), "run id must be made file-name safe")
        self.assertFalse(any(KEY in line for line in logs.output))

    def test_entry_point_without_a_key_stops_with_a_hint(self):
        args = ingest_api.parse_args(["--source", "carrier", "--base-url", "http://x", "--landing", "x"])
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit) as caught:
            ingest_api.run(args)
        self.assertIn("CARRIER_API_KEY", str(caught.exception))


class TestLandingWriters(unittest.TestCase):

    def test_dbutils_landing_puts_one_ndjson_file_per_page(self):
        dbutils = mock.Mock()
        landing = DbutilsLanding("abfss://landing@acct.dfs.core.windows.net/shipments/", dbutils)
        path = landing.write("api_run_p0001.jsonl", [{"a": 1}, {"a": 2}])
        self.assertEqual(path, "abfss://landing@acct.dfs.core.windows.net/shipments/api_run_p0001.jsonl")
        dbutils.fs.put.assert_called_once_with(path, '{"a": 1}\n{"a": 2}\n', True)

    def test_local_landing_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            LocalLanding(Path(tmp) / "shipments").write("p1.jsonl", [{"a": "ü"}])
            files = sorted(p.name for p in (Path(tmp) / "shipments").iterdir())
            self.assertEqual(files, ["p1.jsonl"])
            self.assertEqual((Path(tmp) / "shipments" / "p1.jsonl").read_text(encoding="utf-8"), '{"a": "ü"}\n')

    def test_run_ids_become_safe_file_names(self):
        self.assertEqual(safe_run_id("123456789"), "123456789")
        self.assertEqual(safe_run_id("a/b:c d"), "a_b_c_d")
        self.assertEqual(safe_run_id(""), "run")


if __name__ == "__main__":
    unittest.main()
