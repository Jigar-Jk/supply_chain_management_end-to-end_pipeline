"""
test_bundle_config.py – The Databricks Asset Bundle is wired correctly, checked offline on every PR.

`databricks bundle validate` needs a workspace login, so in CI it cannot catch most mistakes in the
job YAML. These tests read databricks.yml and databricks/resources/*.yml directly and check what would
otherwise only fail on Azure, mid-session:

    entry points exist in the wheel        task flags are accepted by the command
    depends_on points at real tasks        SQL files exist
    every ${var.x} is defined              SLA check runs before the daily pipeline
    qa / prod schedules are paused         clusters use Unity Catalog mode; failures e-mail someone
    no secrets written into the YAML       prod runs as the pipeline service principal

Run locally with:
    python -m unittest tests.test_bundle_config -v
"""

import re
import subprocess
import sys
import tomllib
import unittest
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "databricks.yml"
RESOURCES = sorted((ROOT / "databricks" / "resources").glob("*.yml"))
VAR_REF = re.compile(r"\$\{var\.([a-z_]+)\}")


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def jobs() -> dict:
    """{job key: (job definition, file it is defined in)} across all included resource files."""
    out = {}
    for f in RESOURCES:
        for key, job in (load(f).get("resources", {}).get("jobs") or {}).items():
            out[key] = (job, f)
    return out


def tasks():
    for key, (job, f) in jobs().items():
        for task in job.get("tasks", []):
            yield key, task, f


@lru_cache(maxsize=None)
def accepted_flags(module: str) -> frozenset:
    """The --flags a command really accepts, read from its argparse --help."""
    out = subprocess.run([sys.executable, "-m", module, "--help"], capture_output=True, text=True, cwd=ROOT, timeout=120)
    assert out.returncode == 0, f"{module} --help failed: {out.stderr[-500:]}"
    return frozenset(re.findall(r"(--[a-z][a-z0-9-]*)", out.stdout))


def quartz_time(expr: str) -> tuple:
    """'0 30 2 * * ?' -> (2, 30): hour and minute of a daily Quartz schedule."""
    _sec, minute, hour, *_ = expr.split()
    return int(hour), int(minute)


class TestBundleConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bundle = load(BUNDLE)
        cls.jobs = jobs()
        scripts = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]
        cls.entry_points = {name: target.split(":")[0] for name, target in scripts.items()}   # name -> module

    def test_bundle_includes_the_four_jobs(self):
        self.assertEqual(self.bundle["include"], ["databricks/resources/*.yml"])
        self.assertEqual(sorted(self.jobs), ["scm_daily_pipeline", "scm_reprocess", "scm_streaming", "scm_wms_sla_check"])

    # ------------------------------------------------------------------ wheel tasks

    def test_entry_points_exist_in_the_wheel(self):
        for job, task, _ in tasks():
            if "python_wheel_task" in task:
                wheel = task["python_wheel_task"]
                self.assertEqual(wheel["package_name"], "scm_platform", (job, task["task_key"]))
                self.assertIn(wheel["entry_point"], self.entry_points, (job, task["task_key"]))

    def test_task_flags_are_accepted_by_the_command(self):
        for job, task, _ in tasks():
            if "python_wheel_task" not in task:
                continue
            wheel = task["python_wheel_task"]
            module = self.entry_points.get(wheel["entry_point"])
            if module is None:
                continue                      # reported by test_entry_points_exist_in_the_wheel
            flags = {p for p in wheel.get("parameters", []) if isinstance(p, str) and p.startswith("--")}
            unknown = flags - accepted_flags(module)
            self.assertFalse(unknown, f"{job}.{task['task_key']}: {module} does not accept {sorted(unknown)}")

    def test_wheel_tasks_install_the_wheel(self):
        for job, task, _ in tasks():
            if "python_wheel_task" in task:
                self.assertIn({"whl": "../../dist/*.whl"}, task.get("libraries", []), (job, task["task_key"]))

    # ------------------------------------------------------------------ task graph and files

    def test_depends_on_points_at_real_tasks_without_cycles(self):
        for key, (job, _) in self.jobs.items():
            graph = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in job["tasks"]}
            for task, deps in graph.items():
                for dep in deps:
                    self.assertIn(dep, graph, f"{key}.{task} depends on unknown task {dep}")
            state = {}

            def visit(node, path=()):
                self.assertNotEqual(state.get(node), "open", f"{key}: dependency cycle {path + (node,)}")
                if state.get(node) == "done":
                    return
                state[node] = "open"
                for dep in graph[node]:
                    visit(dep, path + (node,))
                state[node] = "done"

            for node in graph:
                visit(node)

    def test_sql_files_exist(self):
        for job, task, f in tasks():
            sql_file = task.get("sql_task", {}).get("file", {}).get("path")
            if sql_file:
                self.assertTrue((f.parent / sql_file).resolve().is_file(), f"{job}.{task['task_key']}: {sql_file} missing")

    # ------------------------------------------------------------------ variables

    def test_every_variable_used_is_defined(self):
        defined = set(self.bundle["variables"])
        used = set()
        for path in [BUNDLE, *RESOURCES]:
            used |= set(VAR_REF.findall(path.read_text(encoding="utf-8")))
        self.assertFalse(used - defined, f"used but not defined in databricks.yml: {sorted(used - defined)}")

    def test_every_target_sets_its_catalog(self):
        for name, target in self.bundle["targets"].items():
            self.assertEqual(target["variables"]["catalog"], f"scm_platform_{name}")

    def test_shared_unity_catalog_objects_are_not_in_the_bundle(self):
        """Dev is deployed by each developer and by CD; an object only job names keep apart (a volume, a
        table) would be created twice in the same catalog. Such objects belong to Terraform."""
        for f in [BUNDLE, *RESOURCES]:
            kinds = set((load(f).get("resources") or {})) - {"jobs"}
            self.assertFalse(kinds, f"{f.name}: {sorted(kinds)} - declare these in infra/ instead")
        tf = (ROOT / "infra" / "unity_catalog.tf").read_text(encoding="utf-8")
        self.assertIn('resource "databricks_volume" "checkpoints"', tf)
        for _, task, _ in tasks():
            params = (task.get("python_wheel_task") or {}).get("parameters", [])
            if "--checkpoint-root" in params:
                self.assertTrue(params[params.index("--checkpoint-root") + 1].startswith(
                    "/Volumes/${var.catalog}/ops/checkpoints/"), task["task_key"])

    def test_prod_runs_as_the_pipeline_service_principal(self):
        """Prod is written only by scm-pipeline, whoever deploys it; dev runs as the developer."""
        targets = self.bundle["targets"]
        self.assertEqual(targets["prod"].get("run_as"), {"service_principal_name": "${var.pipeline_sp}"})
        self.assertNotIn("run_as", targets["dev"])
        self.assertNotIn("user_name", str(targets["prod"].get("run_as")))

    # ------------------------------------------------------------------ schedules and cost

    def test_sla_check_runs_before_the_daily_pipeline(self):
        sla = quartz_time(self.jobs["scm_wms_sla_check"][0]["schedule"]["quartz_cron_expression"])
        daily = quartz_time(self.jobs["scm_daily_pipeline"][0]["schedule"]["quartz_cron_expression"])
        self.assertLess(sla, daily, "the WMS SLA check must carry stale stock forward before the daily run")
        for key, (job, _) in self.jobs.items():
            self.assertEqual(job["schedule"]["timezone_id"], "UTC", key)

    def test_qa_and_prod_schedules_are_paused(self):
        """Student budget and a one-cluster quota: only manual runs outside dev (which pauses by itself)."""
        targets = self.bundle["targets"]
        self.assertEqual(targets["dev"]["mode"], "development")
        for name in ("qa", "prod"):
            pause = (targets[name].get("presets") or {}).get("trigger_pause_status")
            self.assertEqual(pause, "PAUSED", f"{name}: schedules would run by themselves and use up the credit")

    def test_jobs_are_safe_to_run(self):
        for key, (job, _) in self.jobs.items():
            self.assertEqual(job.get("max_concurrent_runs"), 1, f"{key}: overlapping runs would double-write Silver")
            self.assertIn("${var.alert_email}", job.get("email_notifications", {}).get("on_failure", []), key)
            for cluster in job.get("job_clusters", []):
                spec = cluster["new_cluster"]
                self.assertEqual(spec.get("data_security_mode"), "SINGLE_USER",
                                 f"{key}: without Unity Catalog mode the cluster cannot read abfss:// (no keys)")
                self.assertEqual(spec.get("node_type_id"), "${var.node_type}", key)

    # ------------------------------------------------------------------ secrets

    def test_no_secrets_in_the_yaml(self):
        patterns = {
            "Event Hubs / storage key": r"(SharedAccessKey|AccountKey)\s*=",
            "Databricks token": r"\bdapi[0-9a-f]{32}\b",
            "password value": r"(?i)password\s*[:=]\s*['\"]?[^\s'\"{$][^\s'\"]+",
        }
        for path in [BUNDLE, *RESOURCES]:
            text = path.read_text(encoding="utf-8")
            for what, pattern in patterns.items():
                self.assertIsNone(re.search(pattern, text), f"{path.name}: looks like a {what}")
        for key, (job, _) in self.jobs.items():
            for cluster in job.get("job_clusters", []):
                for name, value in (cluster["new_cluster"].get("spark_env_vars") or {}).items():
                    self.assertRegex(str(value), r"^\{\{secrets/[^/]+/[^}]+\}\}$", f"{key}: {name} must come from a secret scope")


if __name__ == "__main__":
    unittest.main()
