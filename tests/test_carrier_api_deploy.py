"""
test_carrier_api_deploy.py – The carrier API on Azure Container Apps (infra/carrier_api.tf, the
Dockerfile, scripts/up.ps1 step 6b), checked offline.

Container Apps only runs in Azure, so these tests read the files and check the design: no registry
password, the app pulls with its own identity, HTTPS only, the key comes from the secret Databricks
already has, the image serves the session's shipments, and up.ps1 switches the API off (shipments from
the file) when it cannot build the image.

Run locally with:
    python -m unittest tests.test_carrier_api_deploy -v
"""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "infra"
TF = (INFRA / "carrier_api.tf").read_text(encoding="utf-8")
OUTPUTS = (INFRA / "outputs.tf").read_text(encoding="utf-8")
DOCKERFILE = (ROOT / "services" / "carrier_api" / "Dockerfile").read_text(encoding="utf-8")
DOCKERIGNORE = (ROOT / "services" / "carrier_api" / "Dockerfile.dockerignore").read_text(encoding="utf-8")
UP = (ROOT / "scripts" / "up.ps1").read_text(encoding="utf-8")


def strip_comments(text: str) -> str:
    return re.sub(r"#.*", "", text)


def block(text: str, header: str) -> str:
    """The body of the first block that starts with `header` (braces balanced)."""
    start = text.index(header)
    depth, i = 0, text.index("{", start)
    for j in range(i, len(text)):
        depth += {"{": 1, "}": -1}.get(text[j], 0)
        if depth == 0:
            return text[i:j + 1]
    raise AssertionError(f"unbalanced {header}")


CODE = strip_comments(TF)
APP = block(CODE, 'resource "azurerm_container_app" "carrier_api"')


class TestInfrastructure(unittest.TestCase):

    def test_registry_has_no_admin_password(self):
        self.assertRegex(block(CODE, 'resource "azurerm_container_registry" "acr"'), r"admin_enabled\s*=\s*false")

    def test_app_pulls_with_a_read_only_token_for_its_repository(self):
        scope = block(CODE, 'resource "azurerm_container_registry_scope_map" "carrier_api_pull"')
        actions = set(re.findall(r'"(repositories/[^"]+)"', scope))
        self.assertEqual(actions, {"repositories/carrier-api/content/read", "repositories/carrier-api/metadata/read"})
        registry = block(APP, "registry {")
        self.assertIn("azurerm_container_registry_token.carrier_api_pull.name", registry)
        self.assertRegex(registry, r'password_secret_name\s*=\s*"acr-pull-token"')
        self.assertIn("azurerm_container_registry_token_password.carrier_api_pull.password1[0].value", APP)

    def test_no_token_password_or_key_in_outputs(self):
        code = strip_comments(OUTPUTS)
        for name in ("carrier_api_url", "carrier_api_deploy"):
            body = block(code, f'output "{name}"')
            self.assertNotIn("password", body)
            self.assertNotIn("random_password", body)

    def test_https_only_and_reachable_from_databricks(self):
        ingress = block(APP, "ingress {")
        self.assertRegex(ingress, r"external_enabled\s*=\s*true")
        self.assertRegex(ingress, r"allow_insecure_connections\s*=\s*false")
        self.assertRegex(ingress, r"target_port\s*=\s*80\b")

    def test_key_is_the_one_databricks_has_and_only_a_secret_reference_in_env(self):
        self.assertIn("random_password.carrier_api_key.result", APP)
        env = block(APP, "env {")
        self.assertIn('"CARRIER_API_KEY"', env)
        self.assertRegex(env, r'secret_name\s*=\s*"carrier-api-key"')
        self.assertNotRegex(env, r"\bvalue\s*=")

    def test_one_always_on_replica(self):
        self.assertRegex(APP, r"min_replicas\s*=\s*1")
        self.assertRegex(APP, r"max_replicas\s*=\s*1")

    def test_image_is_a_variable_with_a_public_placeholder(self):
        self.assertRegex(APP, r"image\s*=\s*var\.carrier_api_image")
        variables = (INFRA / "variables.tf").read_text(encoding="utf-8")
        self.assertIn('default     = "mcr.microsoft.com/k8se/quickstart:latest"',
                      block(variables, 'variable "carrier_api_image"'))

    def test_only_express_managed_settings_are_ignored(self):
        ignored = set(re.findall(r"(\w+\[0\]\.\w+)", re.search(r"ignore_changes\s*=\s*\[(.*?)\n\s*\]", APP, re.S).group(1)))
        self.assertEqual(ignored, {"ingress[0].transport", "ingress[0].traffic_weight",
                                   "template[0].cooldown_period_in_seconds", "template[0].polling_interval_in_seconds"})

    def test_container_apps_provider_is_registered_but_never_unregistered(self):
        """Found on Azure: a registration resource unregistered Microsoft.App on destroy, Azure said 409."""
        providers = strip_comments((INFRA / "providers.tf").read_text(encoding="utf-8"))
        self.assertRegex(providers, r'resource_providers_to_register\s*=\s*\["Microsoft.App"\]')
        for tf in INFRA.glob("*.tf"):
            self.assertNotIn("azurerm_resource_provider_registration", strip_comments(tf.read_text(encoding="utf-8")), tf.name)

    def test_pipeline_switch_points_at_the_app_over_https(self):
        out = block(strip_comments(OUTPUTS), 'output "carrier_api_url"')
        self.assertIn('"https://${azurerm_container_app.carrier_api.ingress[0].fqdn}"', out)
        self.assertNotIn("sensitive", out)


class TestImage(unittest.TestCase):

    def test_versions_match_requirements_dev(self):
        reqs = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
        for package in ("fastapi", "uvicorn"):
            pinned = re.search(rf"^{package}==([\w.]+)", reqs, re.M).group(1)
            self.assertIn(f"{package}=={pinned}", DOCKERFILE)

    def test_serves_the_sessions_shipments_on_the_ingress_port(self):
        self.assertIn("COPY data/shipments/shipments.jsonl data/shipments/shipments.jsonl", DOCKERFILE)
        self.assertIn('"services.carrier_api.app:create_app", "--factory"', DOCKERFILE)
        self.assertIn('"--port", "80"', DOCKERFILE)

    def test_no_key_in_the_image(self):
        self.assertNotIn("CARRIER_API_KEY", re.sub(r"#.*", "", DOCKERFILE))

    def test_build_context_sends_only_what_is_copied(self):
        lines = [line for line in DOCKERIGNORE.splitlines() if line and not line.startswith("#")]
        self.assertEqual(lines[0], "*")
        copied = set(re.findall(r"^COPY (.+) \S+$", DOCKERFILE, re.M))
        allowed = {line[1:] for line in lines[1:]}
        self.assertEqual({p for c in copied for p in c.split()}, allowed)


class TestUpScript(unittest.TestCase):

    def test_image_is_built_after_the_shipments_are_generated(self):
        self.assertLess(UP.index("generate_shipments"), UP.index("docker build"))
        self.assertLess(UP.index("docker push"), UP.index('"-var=carrier_api_image=$image"'))

    def test_image_rolled_out_by_terraform_not_az_containerapp(self):
        """Found on Azure: `az containerapp update` dropped the registry identity and the pull failed."""
        code = re.sub(r"#.*", "", UP)
        self.assertNotIn("az containerapp update", code)
        rollout = code[code.index('"-var=carrier_api_image=$image"'):]
        self.assertIn("-target=azurerm_container_app.carrier_api", rollout[:300])

    def test_api_off_without_docker(self):
        check = UP[UP.index("docker info"):UP.index("$rg = $out.resource_group.value")]
        self.assertIn('$carrierApi = "disabled"', check)
        self.assertLess(UP.index("docker info"), UP.index("CARRIER_API_URL"))   # before CD is told the URL

    def test_shipments_file_not_uploaded_while_the_api_is_on(self):
        self.assertIn('if ($carrierApi -ne "disabled") { $feeds = $feeds | Where-Object { $_ -ne "shipments" } }', UP)

    def test_waits_for_the_real_api_before_the_pipeline_can_call_it(self):
        self.assertIn('"$carrierApi/health"', UP)
        self.assertLess(UP.index('"$carrierApi/health"'), UP.index("bundle deploy -t $Target"))


if __name__ == "__main__":
    unittest.main()
