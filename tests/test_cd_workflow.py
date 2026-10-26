"""
test_cd_workflow.py – The CD pipeline is wired as designed, checked offline on every PR.

CD only really runs during an Azure session, so these tests catch on every PR what would otherwise
fail there: the job order dev -> qa -> prod, the approval gate on prod, the session values that
scripts/up.ps1 stores under exactly the names .github/workflows/cd.yml reads, and that the pipeline's
OAuth secret is never written into a file, a command line or a public log.

Run locally with:
    python -m unittest tests.test_cd_workflow -v
"""

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CD = ROOT / ".github" / "workflows" / "cd.yml"
UP = ROOT / "scripts" / "up.ps1"
DOWN = ROOT / "scripts" / "down.ps1"
GITHUB_ENVS = ROOT / "scripts" / "github_envs.ps1"
DEPLOY_JOBS = ("dev", "qa", "prod")


def code(path: Path) -> str:
    """A PowerShell script without its comments, so a comment cannot satisfy a check."""
    text = re.sub(r"(?s)<#.*?#>", "", path.read_text(encoding="utf-8"))
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


class TestCdWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.text = CD.read_text(encoding="utf-8")
        cls.wf = yaml.safe_load(cls.text)
        cls.jobs = cls.wf["jobs"]

    def steps(self, job):
        return self.jobs[job]["steps"]

    def runs(self, job):
        return "\n".join(s.get("run", "") for s in self.steps(job))

    def test_started_by_hand_only(self):
        triggers = self.wf[True]   # YAML 1.1 reads the key `on` as True
        self.assertEqual(list(triggers), ["workflow_dispatch"], "the workspace only exists during a session")
        self.assertEqual(triggers["workflow_dispatch"]["inputs"]["run_pipeline"]["type"], "boolean")

    def test_jobs_run_in_order_build_dev_qa_prod(self):
        self.assertEqual(list(self.jobs), ["build", *DEPLOY_JOBS])
        self.assertEqual([self.jobs[j].get("needs") for j in DEPLOY_JOBS], ["build", "dev", "qa"])

    def test_each_deploy_job_uses_its_github_environment(self):
        for job in DEPLOY_JOBS:
            self.assertEqual(self.jobs[job]["environment"], job, "prod's environment is the approval gate")
        self.assertNotIn("environment", self.jobs["build"])

    def test_each_environment_gets_schema_first_then_code(self):
        for job in DEPLOY_JOBS:
            runs = [s.get("run", "") for s in self.steps(job)]
            migrate = next(i for i, r in enumerate(runs) if "apply_migrations.py" in r and "--dry-run" not in r)
            deploy = next(i for i, r in enumerate(runs) if f"bundle deploy -t {job}" in r)
            self.assertLess(migrate, deploy, job)
            self.assertIn(f"--catalog scm_platform_{job}", runs[migrate])

    def test_prod_plan_is_shown_before_the_approval(self):
        self.assertIn("--catalog scm_platform_prod --dry-run", self.runs("qa"))
        self.assertNotIn("--dry-run", self.runs("prod"))

    def test_only_dev_runs_the_pipeline(self):
        run_step = next(s for s in self.steps("dev") if "bundle run" in s.get("run", ""))
        self.assertEqual(run_step["if"], "inputs.run_pipeline")
        for job in ("qa", "prod"):
            self.assertNotIn("bundle run", self.runs(job), "one-cluster quota: qa and prod are only deployed")

    def test_bundle_is_validated_for_every_target(self):
        validate = self.runs("build")
        self.assertIn("bundle validate", validate)
        self.assertIn("for target in dev qa prod", validate)

    def test_stops_early_when_no_session_is_running(self):
        first = self.steps("build")[0]
        self.assertIn("up.ps1", first["name"] + first["run"])
        for name in ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
            self.assertIn(name, first["run"])

    def test_read_only_token_and_one_run_at_a_time(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        self.assertEqual(self.wf["concurrency"]["cancel-in-progress"], False)

    def test_actions_and_cli_are_pinned(self):
        for job in self.jobs.values():
            for step in job["steps"]:
                if "uses" in step:
                    self.assertRegex(step["uses"], r"@v\d+(\.\d+){0,2}$", f"unpinned action: {step['uses']}")
                if step.get("uses", "").startswith("databricks/setup-cli@"):
                    self.assertEqual(step["uses"], f"databricks/setup-cli@v{self.wf['env']['DATABRICKS_CLI_VERSION']}")
        self.assertRegex(self.wf["env"]["DATABRICKS_CLI_VERSION"], r"^\d+\.\d+\.\d+$")

    def test_login_is_the_pipeline_service_principal(self):
        env = self.wf["env"]
        self.assertEqual(env["DATABRICKS_CLIENT_SECRET"], "${{ secrets.DATABRICKS_CLIENT_SECRET }}")
        self.assertEqual(env["BUNDLE_VAR_pipeline_sp"], env["DATABRICKS_CLIENT_ID"], "prod run_as = the CD login")
        self.assertNotIn("DATABRICKS_TOKEN", self.text)

    def test_public_logs_show_no_url_or_e_mail(self):
        """The repo is public, so are its Actions logs; only secrets are masked there."""
        env = self.wf["env"]
        self.assertEqual(env["DATABRICKS_HOST"], "${{ secrets.DATABRICKS_HOST }}")
        self.assertEqual(env["BUNDLE_VAR_alert_email"], "${{ secrets.ALERT_EMAIL }}")

    def test_no_literal_secrets_or_hosts(self):
        self.assertNotRegex(self.text, r"azuredatabricks\.net|dapi[0-9a-f]{8}|@[a-z0-9-]+\.(com|in|org)")

    def test_every_bundle_variable_it_sets_exists(self):
        bundle_vars = set(yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))["variables"])
        used = set(re.findall(r"BUNDLE_VAR_([a-z_]+)", self.text))
        self.assertFalse(used - bundle_vars, f"not variables of databricks.yml: {sorted(used - bundle_vars)}")
        self.assertEqual(bundle_vars - used, {"catalog", "spark_version", "node_type"},
                         "catalog comes from the target; runtime and VM size keep their defaults")


class TestSessionValues(unittest.TestCase):
    """scripts/up.ps1 stores what cd.yml reads, under the same names and of the same kind."""

    @classmethod
    def setUpClass(cls):
        text = CD.read_text(encoding="utf-8")
        cls.cd_vars = set(re.findall(r"\$\{\{\s*vars\.([A-Z_]+)", text))
        cls.cd_secrets = set(re.findall(r"\$\{\{\s*secrets\.([A-Z_]+)", text))
        cls.up = code(UP)
        cls.up_vars = set(re.findall(r"^\s*([A-Z_]+)\s*=", cls.up, re.M)) | set(re.findall(r"gh variable set ([A-Z_]+)", cls.up))
        cls.up_secrets = set(re.findall(r"gh secret set ([A-Z_]+)", cls.up))

    def test_every_secret_cd_reads_is_set_by_up(self):
        self.assertEqual(self.cd_secrets, self.up_secrets)

    def test_every_variable_cd_reads_is_set_by_up(self):
        self.assertFalse(self.cd_vars - self.up_vars, f"cd.yml reads, up.ps1 never sets: {sorted(self.cd_vars - self.up_vars)}")

    def test_alert_id_is_set_per_environment(self):
        self.assertIn('foreach ($envName in "dev", "qa", "prod")', self.up)
        self.assertIn("gh variable set QUARANTINE_ALERT_ID --env $envName", self.up)

    def test_oauth_secret_goes_from_terraform_straight_to_github(self):
        line = next(ln for ln in self.up.splitlines() if "gh secret set DATABRICKS_CLIENT_SECRET" in ln)
        self.assertRegex(line, r'cmd /c ".*terraform .*output -raw pipeline_client_secret \| gh secret set DATABRICKS_CLIENT_SECRET"')
        self.assertNotIn("--body", line)
        self.assertNotRegex(self.up, r"Write-(Host|Output)[^\n]*pipeline_client_secret")

    def test_secret_is_dropped_from_the_outputs_json(self):
        self.assertIn('$out.PSObject.Properties.Remove("pipeline_client_secret")', self.up)

    def test_session_works_without_github(self):
        self.assertIn("[switch]$SkipGitHub", self.up)
        self.assertRegex(self.up, r"Write-Warning \"GitHub CLI not logged in")

    def test_down_removes_the_dead_credentials(self):
        down = code(DOWN)
        self.assertIn('foreach ($name in "DATABRICKS_HOST", "DATABRICKS_CLIENT_SECRET")', down)
        self.assertIn("gh secret delete $name", down)
        self.assertLess(down.index("terraform"), down.index("gh secret delete"), "only after the destroy")


class TestGithubEnvironments(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ps = code(GITHUB_ENVS)

    def test_dev_and_qa_have_no_rules(self):
        self.assertIn('foreach ($name in "dev", "qa")', self.ps)
        self.assertIn("deployment_branch_policy = $null; reviewers = @()", self.ps)

    def test_prod_needs_a_reviewer_who_may_approve_own_runs(self):
        prod = self.ps[self.ps.index('environments/prod" @{'):]
        self.assertIn('reviewers                = @(@{ type = "User"', prod)
        self.assertIn("prevent_self_review      = $false", prod, "one maintainer must be able to approve")

    def test_prod_deploys_from_main_only(self):
        self.assertIn("custom_branch_policies = $true", self.ps)
        self.assertIn('@{ name = "main"; type = "branch" }', self.ps)
        self.assertIn('Where-Object { $_.name -ne "main" }', self.ps)

    def test_environment_names_match_the_workflow(self):
        configured = {"dev", "qa"} | set(re.findall(r"environments/(prod)\"", self.ps))
        used = {j["environment"] for j in yaml.safe_load(CD.read_text(encoding="utf-8"))["jobs"].values()
                if "environment" in j}
        self.assertEqual(configured, used)


if __name__ == "__main__":
    unittest.main()
