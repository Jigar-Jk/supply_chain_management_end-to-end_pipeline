"""
test_api_client.py – The shared REST client (scm_platform/api/client.py): retries, backoff, pagination.

No network: a fake session answers each call from a script (a status, an exception, ...), and a fake
sleep records every wait instead of waiting, so the tests check exactly how long the client would
have paused. The jitter source is pinned to its middle value (factor 1.0) unless a test says otherwise.

Run locally with:
    python -m unittest tests.test_api_client -v
"""

import logging
import unittest

import requests

from scm_platform.api.client import ApiClient, ApiError, parse_retry_after

KEY = "secret-key-123"
BASE = "http://carrier.test"


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, text=None):
        self.status_code, self.headers = status, headers or {}
        self._body, self.text = body, text if text is not None else str(body)

    def json(self):
        if self._body is None:
            raise ValueError("not JSON")
        return self._body


class FakeSession:
    """Answers GETs in order from `script`: a FakeResponse, an int (status) or an exception to raise."""

    def __init__(self, script):
        self.script, self.calls = list(script), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": headers, "timeout": timeout})
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return FakeResponse(step, body={"ok": True}) if isinstance(step, int) else step


class MiddleRandom:
    """rng.random() -> 0.5, so the jitter factor (0.5 + random) is exactly 1.0."""
    def __init__(self, value=0.5):
        self.value = value

    def random(self):
        return self.value


def make_client(script, rng=None, **kwargs):
    session, waits = FakeSession(script), []
    client = ApiClient(BASE, api_key=KEY, session=session, sleep=waits.append, rng=rng or MiddleRandom(), **kwargs)
    return client, session, waits


class TestGet(unittest.TestCase):

    def test_success_returns_the_json_and_sends_key_and_timeout(self):
        client, session, waits = make_client([FakeResponse(200, {"data": [1]})], timeout=12)
        self.assertEqual(client.get("/v1/shipments", {"limit": 5}), {"data": [1]})
        call = session.calls[0]
        self.assertEqual(call["url"], f"{BASE}/v1/shipments")
        self.assertEqual(call["params"], {"limit": 5})
        self.assertEqual(call["headers"]["X-API-Key"], KEY)
        self.assertEqual(call["timeout"], 12)
        self.assertEqual(waits, [])

    def test_server_errors_are_retried_with_exponential_backoff(self):
        client, session, waits = make_client([500, 502, 503, 504, FakeResponse(200, {"ok": 1})])
        self.assertEqual(client.get("x"), {"ok": 1})
        self.assertEqual(len(session.calls), 5)
        self.assertEqual(waits, [1, 2, 4, 8])

    def test_429_waits_for_retry_after(self):
        client, _, waits = make_client([FakeResponse(429, headers={"Retry-After": "7"}), 200])
        client.get("x")
        self.assertEqual(waits, [7])

    def test_429_without_retry_after_backs_off(self):
        client, _, waits = make_client([429, 429, 200])
        client.get("x")
        self.assertEqual(waits, [1, 2])

    def test_a_huge_retry_after_is_capped(self):
        client, _, waits = make_client([FakeResponse(429, headers={"Retry-After": "3600"}), 200], max_retry_after=60)
        client.get("x")
        self.assertEqual(waits, [60])

    def test_client_errors_are_not_retried(self):
        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status):
                client, session, waits = make_client([FakeResponse(status, text="nope")])
                with self.assertRaises(ApiError) as caught:
                    client.get("x")
                self.assertEqual(caught.exception.status, status)
                self.assertEqual((len(session.calls), waits), (1, []))

    def test_retries_run_out_with_a_clear_error(self):
        client, session, waits = make_client([503] * 6, max_retries=5, backoff_max=10)
        with self.assertRaises(ApiError) as caught:
            client.get("/v1/shipments")
        self.assertEqual(len(session.calls), 6)                  # 1 try + 5 retries
        self.assertEqual(waits, [1, 2, 4, 8, 10])                 # 16 capped at backoff_max
        self.assertEqual(caught.exception.status, 503)
        self.assertIn(f"{BASE}/v1/shipments", str(caught.exception))
        self.assertIn("after 5 retries (last: 503)", str(caught.exception))

    def test_timeouts_and_connection_errors_are_retried(self):
        client, _, waits = make_client([requests.Timeout(), requests.ConnectionError(), 200])
        self.assertEqual(client.get("x"), {"ok": True})
        self.assertEqual(waits, [1, 2])
        client, _, _ = make_client([requests.ConnectionError()] * 3, max_retries=2)
        with self.assertRaises(ApiError) as caught:
            client.get("x")
        self.assertIsNone(caught.exception.status)
        self.assertIn("ConnectionError", str(caught.exception))

    def test_a_2xx_without_json_is_an_error_not_a_retry(self):
        client, session, _ = make_client([FakeResponse(200, body=None, text="<html>")])
        with self.assertRaises(ApiError):
            client.get("x")
        self.assertEqual(len(session.calls), 1)

    def test_jitter_spreads_each_wait_between_half_and_one_and_a_half(self):
        low, _, low_waits = make_client([500, 500, 200], rng=MiddleRandom(0.0))
        high, _, high_waits = make_client([500, 500, 200], rng=MiddleRandom(0.999))
        low.get("x"), high.get("x")
        self.assertEqual(low_waits, [0.5, 1.0])
        self.assertAlmostEqual(high_waits[1], 2 * 1.499, places=6)

    def test_requests_per_minute_spaces_out_calls(self):
        now = [100.0]
        client, _, waits = make_client([200, 200, 200], requests_per_minute=60, clock=lambda: now[0])
        client.get("a")                      # first call: no wait
        now[0] += 0.25
        client.get("b")                      # 0.25 s later: wait the remaining 0.75 s
        now[0] += 5
        client.get("c")                      # long after: no wait
        self.assertEqual(waits, [0.75])

    def test_the_key_never_reaches_logs_or_errors(self):
        client, _, _ = make_client([500, FakeResponse(401, text="bad key")])
        with self.assertLogs("scm_platform.api.client", logging.WARNING) as logs, self.assertRaises(ApiError) as caught:
            client.get("x", {"api_key": KEY})                    # some APIs take keys as query parameters
        for line in logs.output + [str(caught.exception)]:
            self.assertNotIn(KEY, line)


class TestPaginate(unittest.TestCase):

    def test_follows_the_cursor_until_there_is_none(self):
        pages = [FakeResponse(200, {"data": [1, 2], "next_cursor": "c1"}),
                 FakeResponse(200, {"data": [3, 4], "next_cursor": "c2"}),
                 FakeResponse(200, {"data": [5], "next_cursor": None})]
        client, session, _ = make_client(pages)
        params = {"updated_since": "2026-11-01T00:00:00Z"}
        self.assertEqual(list(client.paginate("/v1/shipments", params)), [[1, 2], [3, 4], [5]])
        self.assertEqual([c["params"].get("cursor") for c in session.calls], [None, "c1", "c2"])
        self.assertTrue(all(c["params"]["updated_since"] == "2026-11-01T00:00:00Z" for c in session.calls))
        self.assertEqual(params, {"updated_since": "2026-11-01T00:00:00Z"}, "caller's params were changed")

    def test_field_names_are_configurable(self):
        pages = [FakeResponse(200, {"items": ["a"], "next": "p2"}), FakeResponse(200, {"items": ["b"]})]
        client, session, _ = make_client(pages)
        result = list(client.paginate("x", items_field="items", cursor_field="next", cursor_param="page_token"))
        self.assertEqual(result, [["a"], ["b"]])
        self.assertEqual(session.calls[1]["params"], {"page_token": "p2"})

    def test_a_failure_mid_way_stops_the_pages(self):
        """The caller sees the pages that arrived, then the error: it must not mark the run complete."""
        pages = [FakeResponse(200, {"data": [1], "next_cursor": "c1"}), FakeResponse(401, text="key revoked")]
        client, _, _ = make_client(pages)
        seen = []
        with self.assertRaises(ApiError):
            for page in client.paginate("x"):
                seen.append(page)
        self.assertEqual(seen, [[1]])


class TestRetryAfter(unittest.TestCase):

    def test_seconds_dates_and_garbage(self):
        self.assertEqual(parse_retry_after("7"), 7)
        self.assertIsNone(parse_retry_after(None))
        self.assertIsNone(parse_retry_after("soon"))
        self.assertEqual(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), 0)     # in the past: no wait


if __name__ == "__main__":
    unittest.main()
