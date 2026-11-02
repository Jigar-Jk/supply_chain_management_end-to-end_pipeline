"""
carrier.py – Carrier tracking API -> landing/shipments/, incrementally (the mock API locally, a real carrier later).

One run:
    1. read the watermark (none yet: initial_days back)
    2. page through GET /v1/shipments?updated_since=<watermark>
    3. write each non-empty page as landing/shipments/api_<run_id>_p<NNNN>.jsonl
    4. after the LAST page has landed, save the newest updated_at seen as the new watermark

If any page fails (retries used up, a 4xx), the error propagates and the watermark is not touched:
the next run asks again from the old watermark. Pages that did land are fetched again then, so
Bronze can hold a shipment twice; Silver keeps the latest version per shipment, as for any feed
(at-least-once delivery, idempotent Silver). File names carry the run id, so runs never overwrite
each other's files.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional

from scm_platform.api.client import ApiClient
from scm_platform.api.watermark import initial_watermark, parse_utc, to_iso

log = logging.getLogger(__name__)

SOURCE = "carrier_shipments"
PATH = "/v1/shipments"


@dataclass
class FetchResult:
    watermark_before: str
    watermark_after: str
    pages: int = 0
    rows: int = 0
    files: List[str] = field(default_factory=list)


def newest_change(records: List[dict]) -> Optional[datetime]:
    """The largest updated_at in a page; records without a readable one are skipped (Silver judges them)."""
    times = []
    for r in records:
        try:
            times.append(parse_utc(r["updated_at"]))
        except (KeyError, TypeError, ValueError):
            continue
    return max(times, default=None)


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", run_id)[:64] or "run"


def fetch_shipments(client: ApiClient, store: Any, landing: Any, run_id: str,
                    page_size: int = 1000, initial_days: int = 90) -> FetchResult:
    before = store.read(SOURCE) or initial_watermark(initial_days)
    result = FetchResult(watermark_before=before, watermark_after=before)
    newest: Optional[datetime] = None
    run = safe_run_id(run_id)
    log.info("%s: changes after %s", SOURCE, before)

    for number, page in enumerate(client.paginate(PATH, {"updated_since": before, "limit": page_size}), start=1):
        result.pages = number
        if not page:
            continue
        result.files.append(landing.write(f"api_{run}_p{number:04d}.jsonl", page))
        result.rows += len(page)
        page_newest = newest_change(page)
        if page_newest and (newest is None or page_newest > newest):
            newest = page_newest

    # Only reached when every page has landed
    if newest is not None:
        result.watermark_after = store.write(SOURCE, to_iso(newest), result.rows, run_id)
    log.info("%s: %d rows in %d files, watermark %s -> %s",
             SOURCE, result.rows, len(result.files), result.watermark_before, result.watermark_after)
    return result
