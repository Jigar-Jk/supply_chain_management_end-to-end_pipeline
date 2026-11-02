"""
watermark.py – Where each API source remembers how far it has read ("give me what changed after X").

A watermark is the newest change time the source has delivered: the largest updated_at of the
records landed, never the time the job ran. Data that the carrier records at 02:58 but publishes at
03:01 would be skipped for good if the watermark were "the run at 03:00".

Rules every store follows:
- read(source) returns None the first time; the caller then starts from initial_watermark().
- write() is called only after every page of the run has landed (the adapter's job, not the store's).
- A watermark never moves back: write() keeps the later of the stored and the new value, so a run
  that sees nothing new, or a bug, cannot make the next run fetch less than it should.

Two stores with the same two methods:
    FileWatermarkStore    a JSON file, for local runs
    TableWatermarkStore   ops.api_watermark (Liquibase 017), one row per source, for Databricks jobs
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATE_FILE = PROJECT_ROOT / "data" / "_state" / "api_watermarks.json"
SOURCE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def parse_utc(text: str) -> datetime:
    """ISO 8601 with a time zone ('...Z' or '+01:00') to an aware datetime."""
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError(f"watermark {text!r} has no time zone")
    return value


def to_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def initial_watermark(days_back: int = 90, now: Optional[datetime] = None) -> str:
    """Where a source starts when it has never run: days_back before now."""
    return to_iso((now or datetime.now(timezone.utc)) - timedelta(days=days_back))


def later(stored: Optional[str], new: str) -> str:
    """The later of two watermarks (compared as times, so '...Z' and '+05:30' forms agree)."""
    if stored is None or parse_utc(new) > parse_utc(stored):
        return to_iso(parse_utc(new))
    return stored


def _check_source(source: str) -> None:
    if not SOURCE_NAME.match(source):
        raise ValueError(f"invalid source name {source!r}: lower-case letters, digits and _ only")


class FileWatermarkStore:
    """{source: {"watermark", "rows_loaded", "run_id", "updated_at"}} in one JSON file."""

    def __init__(self, path: Path = DEFAULT_STATE_FILE):
        self.path = Path(path)

    def _load(self) -> dict:
        return json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}

    def read(self, source: str) -> Optional[str]:
        _check_source(source)
        return self._load().get(source, {}).get("watermark")

    def write(self, source: str, watermark: str, rows_loaded: int, run_id: str) -> str:
        _check_source(source)
        state = self._load()
        kept = later(state.get(source, {}).get("watermark"), watermark)
        state[source] = {"watermark": kept, "rows_loaded": rows_loaded, "run_id": run_id,
                         "updated_at": to_iso(datetime.now(timezone.utc))}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)          # atomic: a crash mid-write never leaves half a file
        return kept


class TableWatermarkStore:
    """ops.api_watermark in a Unity Catalog catalog; values are passed as SQL parameters, never pasted in."""

    def __init__(self, spark: Any, catalog: str):
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", catalog):
            raise ValueError(f"invalid catalog name {catalog!r}")
        self.spark, self.table = spark, f"{catalog}.ops.api_watermark"

    def read(self, source: str) -> Optional[str]:
        _check_source(source)
        rows = self.spark.sql(f"SELECT watermark FROM {self.table} WHERE source = :source",
                              args={"source": source}).collect()
        return rows[0]["watermark"] if rows else None

    def write(self, source: str, watermark: str, rows_loaded: int, run_id: str) -> str:
        _check_source(source)
        kept = later(self.read(source), watermark)
        self.spark.sql(
            f"""MERGE INTO {self.table} AS t
                USING (SELECT :source AS source, :watermark AS watermark, CAST(:rows AS BIGINT) AS rows_loaded,
                              :run_id AS run_id, current_timestamp() AS updated_at) AS s
                ON t.source = s.source
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""",
            args={"source": source, "watermark": kept, "rows": rows_loaded, "run_id": run_id},
        )
        return kept
