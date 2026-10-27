"""
test_dashboard.py – The AI/BI dashboard (dashboards/control_tower.lvdash.json) is wired correctly.

A broken reference only shows up as an empty or "invalid" widget in the workspace, so these tests
check offline that every widget reads a real dataset and only fields that dataset selects, that the
queries work in any environment (schema.table, catalog from the bundle) and that viewers query with
their own rights, so grants and PII masking apply.

Run locally with:
    python -m unittest tests.test_dashboard -v
"""

import json
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "dashboards" / "control_tower.lvdash.json"
RESOURCE = ROOT / "databricks" / "resources" / "scm_dashboard.yml"


class TestDashboard(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dash = json.loads(DASHBOARD.read_text(encoding="utf-8"))
        cls.datasets = {d["name"]: " ".join(d["queryLines"]) for d in cls.dash["datasets"]}
        cls.widgets = [item["widget"] for page in cls.dash["pages"] for item in page["layout"]]
        cls.resource = yaml.safe_load(RESOURCE.read_text(encoding="utf-8"))["resources"]["dashboards"]["control_tower"]

    def test_every_widget_reads_an_existing_dataset(self):
        for w in self.widgets:
            for q in w["queries"]:
                self.assertIn(q["query"]["datasetName"], self.datasets, w["name"])

    def test_widgets_only_use_columns_their_query_selects(self):
        for w in self.widgets:
            for q in w["queries"]:
                sql = self.datasets[q["query"]["datasetName"]]
                for field in q["query"]["fields"]:
                    self.assertRegex(sql, rf"\b{field['name']}\b", f"{w['name']}: {field['name']} not in the query")
            used = {e["fieldName"] for e in self.encodings(w)}
            fields = {f["name"] for q in w["queries"] for f in q["query"]["fields"]}
            self.assertLessEqual(used, fields, w["name"])

    @staticmethod
    def encodings(widget):
        enc = widget["spec"].get("encodings", {})
        for value in enc.values():
            yield from (value if isinstance(value, list) else [value])

    def test_widget_spec_versions_the_workspace_accepts(self):
        # Found on Azure: table version 1 shows "Invalid widget definition is imported"
        expected = {"counter": 2, "table": 2, "line": 3, "bar": 3}
        for w in self.widgets:
            self.assertEqual(w["spec"]["version"], expected[w["spec"]["widgetType"]], w["name"])

    def test_queries_name_no_catalog(self):
        """The bundle's dataset_catalog picks the environment; a hard-coded catalog would break that."""
        for name, sql in self.datasets.items():
            self.assertNotIn("scm_platform_", sql, name)
            for ref in re.findall(r"\b(?:FROM|JOIN)\s+([\w.]+)", sql, re.I):
                if "." in ref:
                    self.assertEqual(len(ref.split(".")), 2, f"{name}: {ref} should be schema.table")

    def test_bundle_resource_points_at_this_environment(self):
        self.assertEqual(self.resource["dataset_catalog"], "${var.catalog}")
        self.assertEqual(self.resource["warehouse_id"], "${var.warehouse_id}")
        self.assertTrue((RESOURCE.parent / self.resource["file_path"]).resolve() == DASHBOARD.resolve())

    def test_viewers_query_with_their_own_rights(self):
        self.assertIs(self.resource["embed_credentials"], False, "grants and PII masking must apply to viewers")

    def test_widget_names_are_unique_and_fit_the_grid(self):
        names = [w["name"] for w in self.widgets]
        self.assertEqual(len(names), len(set(names)))
        for page in self.dash["pages"]:
            for item in page["layout"]:
                pos = item["position"]
                self.assertLessEqual(pos["x"] + pos["width"], 6, item["widget"]["name"])


if __name__ == "__main__":
    unittest.main()
