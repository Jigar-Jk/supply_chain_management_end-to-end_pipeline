"""
test_run_log.py – Jobs record their successful steps in ops.pipeline_run_log (scm_platform/run_log.py).

The pipeline SLA tile reads the last SUCCESS per source system. Before, nothing wrote SUCCESS, so
every source showed RED. These tests check the row the helper writes, that streaming drains log
their counts, and that every source in ops.sla_config has a job that logs its success.

Run locally with:
    python -m unittest tests.test_run_log -v
"""

import re
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from scm_platform import run_log
from scm_platform.stream_gps_events import StreamConfig

ROOT = Path(__file__).resolve().parent.parent
RUN_LOG_COLUMNS = ["run_id", "source_system", "step", "layer", "started_at", "ended_at", "rows_read",
                   "rows_written", "rows_quarantined", "status", "error_message"]


class FakeSpark:
    def __init__(self, tables=(), history=None):
        self.tables, self.written = set(tables), []
        self.history = history or {}               # table ref -> DESCRIBE HISTORY rows (dicts)
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.tables)

    def sql(self, statement):
        table = statement.replace("DESCRIBE HISTORY ", "", 1)
        if table not in self.history:
            raise RuntimeError(f"no history for {table}")
        return SimpleNamespace(collect=lambda: self.history[table])

    def table(self, name):
        return SimpleNamespace(schema=f"schema of {name}")

    def createDataFrame(self, rows, schema):
        spark = self

        class Writer:
            def mode(self, m):
                self.m = m
                return self

            def saveAsTable(self, name):
                spark.written.append((name, self.m, rows, schema))
        return SimpleNamespace(write=Writer())


def query(*progress):
    return SimpleNamespace(recentProgress=list(progress))


def commit(operation, timestamp, rows):
    return {"operation": operation, "timestamp": timestamp, "operationMetrics": {"numOutputRows": str(rows)}}


class TestRunLogRow(unittest.TestCase):

    def test_columns_match_migration_013(self):
        ddl = (ROOT / "liquibase" / "changelog" / "changes" / "013-quarantine-lifecycle-and-ops.sql").read_text()
        table = ddl.split("CREATE TABLE IF NOT EXISTS ops.pipeline_run_log", 1)[1].split("USING DELTA", 1)[0]
        self.assertEqual(re.findall(r"^\s{4}(\w+)\s", table, re.M), RUN_LOG_COLUMNS)

    def test_a_success_row(self):
        started = datetime(2026, 11, 3, 3, 0)
        row = run_log.run_log_row("run-7", "wms_batch", "fact_inventory_snapshot", "silver", started,
                                  rows_written=24_000, rows_quarantined=480)
        values = dict(zip(RUN_LOG_COLUMNS, row))
        self.assertEqual((values["run_id"], values["source_system"], values["status"]), ("run-7", "wms_batch", "SUCCESS"))
        self.assertEqual((values["rows_written"], values["rows_quarantined"], values["rows_read"]), (24_000, 480, None))
        self.assertGreaterEqual(values["ended_at"], started)
        self.assertIsNone(values["ended_at"].tzinfo, "timestamps are naive UTC like the rest of the pipeline")


class TestLogStep(unittest.TestCase):

    def test_appends_one_row_to_the_catalogs_run_log(self):
        spark = FakeSpark({"scm_platform_dev.ops.pipeline_run_log"})
        row = run_log.run_log_row("r", "carrier_api", "fact_shipment", "silver", datetime(2026, 11, 3))
        self.assertTrue(run_log.log_step(spark, "scm_platform_dev", row))
        self.assertEqual(spark.written, [("scm_platform_dev.ops.pipeline_run_log", "append", [row],
                                          "schema of scm_platform_dev.ops.pipeline_run_log")])

    def test_never_breaks_a_job_without_the_table(self):
        row = run_log.run_log_row("r", "gps_stream", "stream_gps", "bronze", datetime(2026, 11, 3))
        for spark, catalog in ((FakeSpark(), "scm_platform_dev"), (FakeSpark({"x.ops.pipeline_run_log"}), None)):
            self.assertFalse(run_log.log_step(spark, catalog, row))
            self.assertEqual(spark.written, [])


class TestStreamDrains(unittest.TestCase):

    def test_stream_rows_sums_micro_batches_and_ignores_unknown_sink_counts(self):
        q = query({"numInputRows": 1000, "sink": {"numOutputRows": 990}},
                  {"numInputRows": 500, "sink": {"numOutputRows": -1}},
                  {"numInputRows": 0})
        self.assertEqual(run_log.stream_rows([q]), (1500, 990))

    def test_gps_drain_logs_gps_stream_with_events_and_dead_letters(self):
        from scm_platform.stream_gps_events import log_drain
        spark = FakeSpark({"cat.ops.pipeline_run_log"})
        cfg = StreamConfig(bootstrap_servers="b", catalog="cat", available_now=True)
        bronze = query({"numInputRows": 2000, "sink": {"numOutputRows": 1950}})
        dlq = query({"numInputRows": 2000, "sink": {"numOutputRows": 30}})
        self.assertTrue(log_drain(spark, cfg, bronze, dlq, "run-9", datetime(2026, 11, 3)))
        values = dict(zip(RUN_LOG_COLUMNS, spark.written[0][2][0]))
        self.assertEqual((values["source_system"], values["step"], values["run_id"]), ("gps_stream", "stream_gps", "run-9"))
        self.assertEqual((values["rows_read"], values["rows_written"], values["rows_quarantined"]), (2000, 1950, 30))

    def test_cdc_drain_counts_both_tables_and_their_dead_letters(self):
        from scm_platform.stream_cdc_events import log_drain
        spark = FakeSpark({"cat.ops.pipeline_run_log"})
        cfg = StreamConfig(bootstrap_servers="b", catalog="cat", available_now=True)
        # start_cdc_streams order: order_lines events, its dead letters, inventory events, its dead letters
        queries = [query({"numInputRows": 300, "sink": {"numOutputRows": 300}}), query({"sink": {"numOutputRows": 1}}),
                   query({"numInputRows": 50, "sink": {"numOutputRows": 49}}), query({"sink": {"numOutputRows": 0}})]
        log_drain(spark, cfg, queries, None, datetime(2026, 11, 3))
        values = dict(zip(RUN_LOG_COLUMNS, spark.written[0][2][0]))
        self.assertEqual((values["source_system"], values["run_id"]), ("sqlserver_cdc", "stream"))
        self.assertEqual((values["rows_read"], values["rows_written"], values["rows_quarantined"]), (350, 349, 1))

    def test_sink_without_counts_falls_back_to_the_delta_history(self):
        """Found on Azure: Databricks' Delta sink reports -1, so 19,955 landed GPS events were logged as 0."""
        from scm_platform.stream_gps_events import log_drain
        started = datetime(2026, 11, 3, 9, 0)
        history = {
            "cat.bronze.gps_events": [
                commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 5), 19000),
                commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 6), 955),
                commit("STREAMING UPDATE", datetime(2026, 11, 2, 9, 5), 777),     # an earlier drain
                commit("OPTIMIZE", datetime(2026, 11, 3, 9, 7), 5)],
            "cat.bronze.gps_events_dlq": [commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 5), 45)],
        }
        spark = FakeSpark({"cat.ops.pipeline_run_log"}, history)
        cfg = StreamConfig(bootstrap_servers="b", catalog="cat", available_now=True)
        bronze = query({"numInputRows": 20048, "sink": {"numOutputRows": -1}})
        dlq = query({"numInputRows": 20048, "sink": {"numOutputRows": -1}})
        log_drain(spark, cfg, bronze, dlq, "r", started)
        values = dict(zip(RUN_LOG_COLUMNS, spark.written[0][2][0]))
        self.assertEqual((values["rows_read"], values["rows_written"], values["rows_quarantined"]), (20048, 19955, 45))

    def test_cdc_history_fallback_reads_both_tables(self):
        from scm_platform.stream_cdc_events import log_drain
        started = datetime(2026, 11, 3, 9, 0)
        history = {"cat.bronze.orders_cdc": [commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 1), 300)],
                   "cat.bronze.inventory_cdc": [commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 1), 49)],
                   "cat.bronze.cdc_dlq": [commit("STREAMING UPDATE", datetime(2026, 11, 3, 9, 1), 1)]}
        spark = FakeSpark({"cat.ops.pipeline_run_log"}, history)
        cfg = StreamConfig(bootstrap_servers="b", catalog="cat", available_now=True)
        unknown = {"sink": {"numOutputRows": -1}}
        queries = [query({"numInputRows": 300, **unknown}), query({"numInputRows": 300, **unknown}),
                   query({"numInputRows": 50, **unknown}), query({"numInputRows": 50, **unknown})]
        log_drain(spark, cfg, queries, None, started)
        values = dict(zip(RUN_LOG_COLUMNS, spark.written[0][2][0]))
        self.assertEqual((values["rows_read"], values["rows_written"], values["rows_quarantined"]), (350, 349, 1))

    def test_unreadable_history_keeps_the_progress_count(self):
        spark = FakeSpark()
        q = query({"numInputRows": 10, "sink": {"numOutputRows": -1}})
        self.assertEqual(run_log.drain_rows(spark, [q], ["cat.bronze.missing"], datetime(2026, 11, 3)), (10, 0))

    def test_aware_history_timestamps_compare_as_utc(self):
        from datetime import timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        spark = FakeSpark(history={"t": [commit("STREAMING UPDATE", datetime(2026, 11, 3, 14, 35, tzinfo=ist), 7)]})
        self.assertEqual(run_log.delta_rows_since(spark, ["t"], datetime(2026, 11, 3, 9, 0)), 7)    # 09:05 UTC
        self.assertEqual(run_log.delta_rows_since(spark, ["t"], datetime(2026, 11, 3, 9, 10)), 0)

    def test_local_lake_tables_are_named_by_path(self):
        cfg = StreamConfig(bootstrap_servers="b", lake_path="/lake/")
        self.assertEqual(cfg.history_ref("gps_events"), "delta.`/lake/bronze/gps_events`")

    def test_a_continuous_stream_logs_nothing(self):
        from scm_platform.stream_gps_events import log_drain
        spark = FakeSpark({"cat.ops.pipeline_run_log"})
        cfg = StreamConfig(bootstrap_servers="b", catalog="cat", available_now=False)
        self.assertFalse(log_drain(spark, cfg, query(), query(), "r", datetime(2026, 11, 3)))
        self.assertEqual(spark.written, [])


class TestEverySlaSourceIsLogged(unittest.TestCase):
    """The tile is only as good as its inputs: each source in ops.sla_config needs a job that logs SUCCESS."""

    def test_each_sla_source_has_a_writer(self):
        changes = ROOT / "liquibase" / "changelog" / "changes"
        seed = (changes / "013-quarantine-lifecycle-and-ops.sql").read_text(encoding="utf-8")
        sources = set(re.findall(r"\('(\w+)',\s*(?:'\d\d:\d\d'|NULL)", seed))
        for renamed in re.findall(r"UPDATE ops\.sla_config SET source_system = '(\w+)'.*WHERE source_system = '(\w+)'",
                                  (changes / "015-streaming-sla-and-sqlserver-cdc-name.sql").read_text(encoding="utf-8")):
            sources = (sources - {renamed[1]}) | {renamed[0]}
        self.assertEqual(sources, {"wms_batch", "carrier_api", "sqlserver_cdc", "gps_stream"})

        from scm_platform.silver.quality import FEED_SOURCE_SYSTEM
        pipeline = (ROOT / "scm_platform" / "silver" / "pipeline.py").read_text(encoding="utf-8")
        silver_feeds = re.findall(r'log_fact\("\w+", "(\w+)"', pipeline)
        logged = {FEED_SOURCE_SYSTEM[f] for f in silver_feeds}
        streams = {"gps_stream": ("stream_gps_events.py", '"gps_stream", "stream_gps"'),
                   "sqlserver_cdc": ("stream_cdc_events.py", 'SOURCE_SYSTEM, "stream_cdc"')}
        for source, (module, call) in streams.items():
            if call in (ROOT / "scm_platform" / module).read_text(encoding="utf-8"):
                logged.add(source)
        self.assertEqual(sorted(silver_feeds), ["inventory", "orders", "shipments"])
        self.assertLessEqual(sources, logged, f"no job logs SUCCESS for {sorted(sources - logged)}")

    def test_streaming_tasks_pass_the_job_run_id(self):
        import yaml
        jobs = yaml.safe_load((ROOT / "databricks" / "resources" / "scm_streaming.yml").read_text())["resources"]["jobs"]
        for task in jobs["scm_streaming"]["tasks"]:
            params = task["python_wheel_task"]["parameters"]
            self.assertEqual(params[params.index("--run-id") + 1], "{{job.run_id}}", task["task_key"])


if __name__ == "__main__":
    unittest.main()
