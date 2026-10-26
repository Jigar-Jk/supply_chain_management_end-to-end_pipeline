"""
test_apply_migrations.py – The migration runner applies each changeset once, and refuses edited ones.

scripts/apply_migrations.py keeps a register (ops.schema_changelog) of applied changesets, like
Liquibase's DATABASECHANGELOG. These tests read the real changelog and run the plan / apply logic
against an in-memory register, so no Databricks workspace is needed.

Run locally with:
    python -m unittest tests.test_apply_migrations -v
"""

import io
import os
import re
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from unittest import mock

from scripts import apply_migrations as am

CHANGES_DIR = am.CHANGELOG / "changes"


def run(steps, fail_on=None, dry_run=False):
    """migrate() against an in-memory register; returns (failures, executed SQL, recorded ids, output)."""
    executed, recorded, out = [], [], io.StringIO()

    def execute(sql):
        executed.append(sql)
        return "boom" if fail_on and fail_on in sql else None

    with redirect_stdout(out):
        failures = am.migrate(steps, execute, lambda c: recorded.append(c.id), dry_run=dry_run)
    return failures, executed, recorded, out.getvalue()


class TestChangelog(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.changesets = am.changesets()

    def test_one_changeset_per_marker_plus_the_gold_changesets(self):
        markers = sum(len(re.findall(r"(?m)^-- changeset ", p.read_text(encoding="utf-8")))
                      for p in CHANGES_DIR.glob("*.sql"))
        gold = len(list((am.CHANGELOG / "gold").glob("*.sql")))
        self.assertEqual(len(self.changesets), markers + gold)

    def test_ids_are_unique_and_have_an_author(self):
        ids = [c.id for c in self.changesets]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(re.fullmatch(r"[a-z-]+:[A-Za-z0-9_.-]+", i) for i in ids), ids)

    def test_order_follows_the_master_changelog(self):
        files = [c.file for c in self.changesets]
        self.assertEqual(files[0], "changes/001-create-bronze-tables.sql")
        table_files = [f for f in files if f.startswith("changes/")]
        self.assertEqual(table_files, sorted(table_files))
        self.assertTrue(files.index("gold/06_mart_logistics_kpi_daily.sql") > files.index("gold/01_vw_otif_daily.sql"))
        self.assertTrue(all(f.startswith("gold/") for f in files[len(table_files):]), "Gold comes after all tables")

    def test_only_gold_views_run_on_change(self):
        for c in self.changesets:
            self.assertEqual(c.run_on_change, c.file.startswith("gold/"), c.id)

    def test_every_changeset_has_sql_and_no_comments(self):
        for c in self.changesets:
            self.assertTrue(c.statements, c.id)
            for sql in c.statements:
                self.assertFalse(sql.lstrip().startswith("--"), c.id)
                self.assertFalse(sql.rstrip().endswith(";"), c.id)

    def test_gold_files_are_one_statement(self):
        for c in self.changesets:
            if c.file.startswith("gold/"):
                self.assertEqual(len(c.statements), 1, c.id)

    def test_checksum_ignores_comments_and_line_endings_but_not_sql(self):
        a = am.Changeset("x:1", "f.sql", am.split_statements("-- note\nCREATE TABLE t (a INT);\n"), False)
        b = am.Changeset("x:1", "f.sql", am.split_statements("-- other note\r\nCREATE TABLE t (a INT);  \r\n"), False)
        c = am.Changeset("x:1", "f.sql", am.split_statements("CREATE TABLE t (a BIGINT);"), False)
        self.assertEqual(a.checksum, b.checksum)
        self.assertNotEqual(a.checksum, c.checksum)

    def test_marker_header_is_ignored(self):
        path = CHANGES_DIR / "013-quarantine-lifecycle-and-ops.sql"
        found = am.formatted_sql_changesets(path)
        self.assertEqual(found[0].id, "scm-platform-team:quarantine-lifecycle-v3")
        self.assertTrue(found[0].statements[0].startswith("ALTER TABLE quarantine.records ADD COLUMNS"))


class TestPlanAndApply(unittest.TestCase):

    def setUp(self):
        self.table = am.Changeset("t:table-v1", "changes/900.sql", ("CREATE TABLE a (x INT)", "ALTER TABLE a SET TBLPROPERTIES ('k' = 'v')"), False)
        self.later = am.Changeset("t:table-v2", "changes/901.sql", ("ALTER TABLE a ADD COLUMNS (y INT)",), False)
        self.view = am.Changeset("t:view", "gold/99.sql", ("CREATE OR REPLACE VIEW v AS SELECT 1",), True)
        self.all = [self.table, self.later, self.view]

    def test_fresh_catalog_runs_everything_in_order(self):
        steps = am.plan(self.all, {})
        self.assertEqual([a for _, a in steps], [am.RUN, am.RUN, am.RUN])
        failures, executed, recorded, _ = run(steps)
        self.assertEqual(failures, 0)
        self.assertEqual(executed, [*self.table.statements, *self.later.statements, *self.view.statements])
        self.assertEqual(recorded, ["t:table-v1", "t:table-v2", "t:view"])

    def test_second_run_with_nothing_new_runs_nothing(self):
        applied = {c.id: c.checksum for c in self.all}
        failures, executed, recorded, out = run(am.plan(self.all, applied))
        self.assertEqual((failures, executed, recorded), (0, [], []))
        self.assertIn("run 0, re-run 0, skipped 3", out)

    def test_only_the_new_changeset_runs(self):
        applied = {c.id: c.checksum for c in (self.table, self.view)}
        _, executed, recorded, _ = run(am.plan(self.all, applied))
        self.assertEqual(executed, list(self.later.statements))
        self.assertEqual(recorded, ["t:table-v2"])

    def test_edited_view_is_rerun(self):
        applied = {c.id: c.checksum for c in self.all}
        edited = replace(self.view, statements=("CREATE OR REPLACE VIEW v AS SELECT 2",))
        steps = am.plan([self.table, self.later, edited], applied)
        self.assertEqual(steps[2][1], am.RERUN)
        _, executed, recorded, _ = run(steps)
        self.assertEqual((executed, recorded), (list(edited.statements), ["t:view"]))

    def test_edited_table_changeset_stops_before_running_anything(self):
        applied = {self.table.id: self.table.checksum}
        edited = replace(self.table, statements=("CREATE TABLE a (x BIGINT)",))
        steps = am.plan([edited, self.later, self.view], applied)
        self.assertEqual(steps[0][1], am.EDITED)
        failures, executed, recorded, out = run(steps)
        self.assertGreater(failures, 0)
        self.assertEqual((executed, recorded), ([], []), "nothing may run when an applied changeset was edited")
        self.assertIn("write a new changeset", out)

    def test_failure_stops_and_records_only_what_succeeded(self):
        failures, executed, recorded, out = run(am.plan(self.all, {}), fail_on="ADD COLUMNS")
        self.assertEqual(failures, 1)
        self.assertEqual(recorded, ["t:table-v1"])
        self.assertNotIn(self.view.statements[0], executed, "changesets after a failure must not run")
        self.assertIn("FAIL    t:table-v2", out)

    def test_dry_run_changes_nothing(self):
        failures, executed, recorded, out = run(am.plan(self.all, {self.table.id: self.table.checksum}), dry_run=True)
        self.assertEqual((failures, executed, recorded), (0, [], []))
        self.assertIn("would RUN", out)
        self.assertIn("plan: run 2, re-run 0, skipped 1", out)


class TestLogin(unittest.TestCase):
    HOST = "https://adb-1.azuredatabricks.net"

    def test_service_principal_uses_oauth_client_credentials(self):
        response = mock.Mock(status_code=200)
        response.json.return_value = {"access_token": "tok"}
        env = {"DATABRICKS_CLIENT_ID": "app-id", "DATABRICKS_CLIENT_SECRET": "s3cr3t"}
        with mock.patch.dict(os.environ, env), mock.patch.object(am.requests, "post", return_value=response) as post:
            self.assertEqual(am.access_token(self.HOST, None), "tok")
        args, kwargs = post.call_args
        self.assertEqual(args[0], f"{self.HOST}/oidc/v1/token")
        self.assertEqual(kwargs["auth"], ("app-id", "s3cr3t"))
        self.assertEqual(kwargs["data"], {"grant_type": "client_credentials", "scope": "all-apis"})

    def test_failed_login_does_not_show_the_secret(self):
        env = {"DATABRICKS_CLIENT_ID": "app-id", "DATABRICKS_CLIENT_SECRET": "s3cr3t"}
        with mock.patch.dict(os.environ, env), \
                mock.patch.object(am.requests, "post", return_value=mock.Mock(status_code=401)):
            with self.assertRaises(SystemExit) as ctx:
                am.access_token(self.HOST, None)
        self.assertNotIn("s3cr3t", str(ctx.exception))
        self.assertIn("401", str(ctx.exception))

    def test_profile_wins_over_service_principal(self):
        env = {"DATABRICKS_CLIENT_ID": "app-id", "DATABRICKS_CLIENT_SECRET": "s3cr3t"}
        done = mock.Mock(stdout='{"access_token": "from-profile"}')
        with mock.patch.dict(os.environ, env), mock.patch.object(am.subprocess, "run", return_value=done), \
                mock.patch.object(am.requests, "post") as post:
            self.assertEqual(am.access_token(self.HOST, "scm-azure"), "from-profile")
        post.assert_not_called()

    def test_without_sp_variables_falls_back_to_az_login(self):
        env = {k: v for k, v in os.environ.items() if k not in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")}
        done = mock.Mock(stdout="az-token\n")
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(am.subprocess, "run", return_value=done) as sp:
            self.assertEqual(am.access_token(self.HOST, None), "az-token")
        self.assertEqual(sp.call_args[0][0][:3], ["az", "account", "get-access-token"])


class TestRegisterSql(unittest.TestCase):

    def test_record_is_idempotent_and_quotes_values(self):
        sent = []
        wh = am.Warehouse(session=None, host="h", warehouse="w", catalog="c")
        wh.execute = lambda sql: sent.append(sql)
        wh.record(am.Changeset("a:it's", "changes/x.sql", ("SELECT 1",), False))
        self.assertTrue(sent[0].startswith(f"MERGE INTO {am.REGISTER}"))
        self.assertIn("'a:it''s'", sent[0])
        self.assertIn("current_user()", sent[0])

    def test_register_lives_in_ops(self):
        self.assertEqual(am.REGISTER, "ops.schema_changelog")  # ops exists before any changeset (Terraform)


if __name__ == "__main__":
    unittest.main()
