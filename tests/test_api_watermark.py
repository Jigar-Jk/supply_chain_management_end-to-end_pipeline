"""
test_api_watermark.py – Incremental watermarks for REST sources (scm_platform/api/watermark.py, Liquibase 017).

The file store runs on a real temp file. The table store runs on a fake Spark session that keeps
ops.api_watermark as a dict and records every statement, so the tests check the SQL and its
parameters; the MERGE itself needs Delta and is exercised by the next Azure run.

Run locally with:
    python -m unittest tests.test_api_watermark -v
"""

import json
import re
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scm_platform.api.watermark import (
    FileWatermarkStore,
    TableWatermarkStore,
    initial_watermark,
    later,
)

ROOT = Path(__file__).resolve().parent.parent
CHANGESET = ROOT / "liquibase" / "changelog" / "changes" / "017-api-watermark.sql"


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeSpark:
    """Just enough of spark.sql for the table store: SELECT by source and MERGE on source."""

    def __init__(self):
        self.table, self.statements = {}, []

    def sql(self, query, args=None):
        self.statements.append((query, args))
        if query.lstrip().startswith("SELECT"):
            row = self.table.get(args["source"])
            return FakeResult([{"watermark": row["watermark"]}] if row else [])
        if query.lstrip().startswith("MERGE"):
            self.table[args["source"]] = {"watermark": args["watermark"], "rows_loaded": args["rows"],
                                          "run_id": args["run_id"]}
            return FakeResult([])
        raise AssertionError(f"unexpected SQL: {query}")


class TestWatermarkRules(unittest.TestCase):

    def test_initial_watermark_is_days_back_in_utc(self):
        now = datetime(2026, 11, 2, 3, 0, tzinfo=timezone.utc)
        self.assertEqual(initial_watermark(90, now), "2026-08-04T03:00:00Z")

    def test_later_keeps_the_newest_and_compares_as_times(self):
        self.assertEqual(later(None, "2026-11-02T03:00:00Z"), "2026-11-02T03:00:00Z")
        self.assertEqual(later("2026-11-02T03:00:00Z", "2026-11-01T00:00:00Z"), "2026-11-02T03:00:00Z")
        # 08:00 at +05:30 is 02:30 UTC: earlier than 03:00 UTC, although the string sorts later
        self.assertEqual(later("2026-11-02T03:00:00Z", "2026-11-02T08:00:00+05:30"), "2026-11-02T03:00:00Z")
        self.assertEqual(later("2026-11-02T03:00:00Z", "2026-11-02T09:00:00+05:30"), "2026-11-02T03:30:00Z")

    def test_a_time_without_a_zone_is_refused(self):
        with self.assertRaises(ValueError):
            later(None, "2026-11-02T03:00:00")


class TestFileStore(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state" / "api_watermarks.json"
        self.store = FileWatermarkStore(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_first_read_is_none(self):
        self.assertIsNone(self.store.read("carrier_shipments"))

    def test_write_then_read_and_keep_details(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", rows_loaded=412, run_id="run-1")
        self.assertEqual(self.store.read("carrier_shipments"), "2026-11-02T02:58:00Z")
        saved = json.loads(self.path.read_text())["carrier_shipments"]
        self.assertEqual((saved["rows_loaded"], saved["run_id"]), (412, "run-1"))

    def test_never_moves_back(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 412, "run-1")
        kept = self.store.write("carrier_shipments", "2026-10-01T00:00:00Z", 0, "run-2")
        self.assertEqual(kept, "2026-11-02T02:58:00Z")
        self.assertEqual(self.store.read("carrier_shipments"), "2026-11-02T02:58:00Z")

    def test_sources_are_independent(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 412, "run-1")
        self.store.write("weather_forecast", "2026-11-02T06:00:00Z", 1200, "run-1")
        self.assertEqual(self.store.read("carrier_shipments"), "2026-11-02T02:58:00Z")
        self.assertEqual(self.store.read("weather_forecast"), "2026-11-02T06:00:00Z")

    def test_no_temp_file_is_left_behind(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 1, "run-1")
        self.assertEqual([p.name for p in self.path.parent.iterdir()], ["api_watermarks.json"])

    def test_source_names_are_checked(self):
        for bad in ("Carrier", "carrier-shipments", "x; DROP TABLE", ""):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.store.read(bad)


class TestTableStore(unittest.TestCase):

    def setUp(self):
        self.spark = FakeSpark()
        self.store = TableWatermarkStore(self.spark, "scm_platform_dev")

    def test_first_read_is_none(self):
        self.assertIsNone(self.store.read("carrier_shipments"))

    def test_write_merges_one_row_per_source_and_never_moves_back(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 412, "run-1")
        self.store.write("carrier_shipments", "2026-10-01T00:00:00Z", 0, "run-2")       # older: kept back
        self.store.write("carrier_shipments", "2026-11-03T02:59:00Z", 380, "run-3")
        self.assertEqual(list(self.spark.table), ["carrier_shipments"])
        self.assertEqual(self.store.read("carrier_shipments"), "2026-11-03T02:59:00Z")
        self.assertEqual(self.spark.table["carrier_shipments"]["run_id"], "run-3")

    def test_sql_targets_the_catalog_and_uses_parameters_only(self):
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 412, "run-1")
        for query, args in self.spark.statements:
            self.assertIn("scm_platform_dev.ops.api_watermark", query)
            self.assertNotIn("2026-11-02", query, "values must be parameters, not pasted into SQL")
            self.assertEqual(set(re.findall(r":(\w+)", query)), set(args), query)

    def test_catalog_and_source_names_are_checked(self):
        with self.assertRaises(ValueError):
            TableWatermarkStore(self.spark, "dev; DROP TABLE x")
        with self.assertRaises(ValueError):
            self.store.write("Carrier Shipments", "2026-11-02T02:58:00Z", 1, "run-1")

    def test_the_table_matches_liquibase_017(self):
        ddl = CHANGESET.read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS ops.api_watermark", ddl)
        self.store.write("carrier_shipments", "2026-11-02T02:58:00Z", 1, "run-1")
        merge = next(q for q, _ in self.spark.statements if "MERGE" in q)
        select = merge.split("USING", 1)[1].split(") AS s", 1)[0]
        columns = re.findall(r"\bAS (\w+)\s*(?=,|$)", select.strip())       # aliases, not CAST(... AS BIGINT)
        self.assertEqual(columns, ["source", "watermark", "rows_loaded", "run_id", "updated_at"])
        for column in columns:
            self.assertRegex(ddl, rf"\n\s+{column} ", f"MERGE writes {column}, which 017 does not create")


if __name__ == "__main__":
    unittest.main()
