"""
client.py – HTTP client shared by every REST source (carrier, weather, routes): retries, backoff, pagination.

What it does with each answer:

    2xx                          return the JSON body
    429                          wait for the Retry-After header's seconds (capped), or back off; retry
    500, 502, 503, 504           retry with exponential backoff: 1, 2, 4, 8, 16 s (capped), each +-50 % jitter
    timeout, connection error    same as 5xx
    any other 4xx                no retry: the request itself is wrong (bad key, bad parameter), and
                                 sending it again cannot fix it
    still failing after max_retries   ApiError: the job task fails and the failure e-mail goes out

Jitter spreads retries out, so many clients that failed together do not all come back at the same
moment and knock the server over again. requests_per_minute keeps a client under an API's quota.

The key is sent in a header and never appears in a log line or an error message; errors name the
URL without its query string, which some APIs use for keys.

    client = ApiClient("http://localhost:8080", api_key=key)
    for page in client.paginate("/v1/shipments", {"updated_since": "2026-11-01T00:00:00Z"}):
        ...                                       # page = the "data" list of one response
"""

import email.utils
import logging
import random
import time
from datetime import datetime, timezone
from typing import Callable, Iterator, List, Optional

import requests

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class ApiError(Exception):
    """A request that failed for good: a non-retryable answer, or retries used up."""

    def __init__(self, message: str, status: Optional[int] = None, url: Optional[str] = None):
        super().__init__(message)
        self.status, self.url = status, url


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Retry-After is either seconds ("7") or an HTTP date; None when absent or unreadable."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


class ApiClient:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        key_header: str = "X-API-Key",
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff_base: float = 1.0,
        backoff_max: float = 30.0,
        max_retry_after: float = 60.0,
        requests_per_minute: Optional[float] = None,
        session: Optional[requests.Session] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: Optional[random.Random] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Accept": "application/json"}
        if api_key:
            self.headers[key_header] = api_key
        self.timeout, self.max_retries = timeout, max_retries
        self.backoff_base, self.backoff_max, self.max_retry_after = backoff_base, backoff_max, max_retry_after
        self.min_interval = 60.0 / requests_per_minute if requests_per_minute else 0.0
        self.session = session or requests.Session()
        self.sleep, self.clock, self.rng = sleep, clock, rng or random.Random()
        self._last_request: Optional[float] = None

    def backoff(self, attempt: int) -> float:
        """Seconds before retry number attempt+1: base * 2^attempt, capped, times a random 0.5-1.5."""
        return min(self.backoff_base * 2 ** attempt, self.backoff_max) * (0.5 + self.rng.random())

    def _throttle(self) -> None:
        if self.min_interval and self._last_request is not None:
            wait = self._last_request + self.min_interval - self.clock()
            if wait > 0:
                self.sleep(wait)
        self._last_request = self.clock()

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET base_url + path and return the JSON body, retrying what is worth retrying."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(self.max_retries + 1):
            self._throttle()
            status, wait = None, None
            try:
                response = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                reason = type(exc).__name__
            else:
                status = response.status_code
                if status < 400:
                    try:
                        return response.json()
                    except ValueError:
                        raise ApiError(f"GET {url}: {status} but the body is not JSON", status, url) from None
                if status not in RETRYABLE_STATUS:
                    raise ApiError(f"GET {url}: {status} {response.text[:200]}", status, url)
                reason = str(status)
                if status == 429:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    if retry_after is not None:
                        wait = min(retry_after, self.max_retry_after)
            if attempt == self.max_retries:
                raise ApiError(f"GET {url}: still failing after {self.max_retries} retries (last: {reason})",
                               status, url)
            wait = self.backoff(attempt) if wait is None else wait
            log.warning("GET %s: %s, retry %d/%d in %.1f s", url, reason, attempt + 1, self.max_retries, wait)
            self.sleep(wait)
        raise AssertionError("unreachable")

    def paginate(
        self,
        path: str,
        params: Optional[dict] = None,
        items_field: str = "data",
        cursor_field: str = "next_cursor",
        cursor_param: str = "cursor",
    ) -> Iterator[List[dict]]:
        """Yield each page's items, following the cursor until the API returns none.

        The field names differ between APIs, so they are arguments. The caller's params are not changed.
        """
        params = dict(params or {})
        while True:
            body = self.get(path, params)
            yield body.get(items_field) or []
            cursor = body.get(cursor_field)
            if not cursor:
                return
            params[cursor_param] = cursor
