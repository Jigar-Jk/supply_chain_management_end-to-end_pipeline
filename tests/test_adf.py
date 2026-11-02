"""
test_adf.py – The WMS file transfer as code (infra/wms_sftp.tf, infra/adf.tf, infra/adf/*.json), checked offline.

Data Factory only runs in Azure, so these tests read the Terraform and the pipeline JSON and check
the design: the copy reads the SFTP inbox and writes exactly where Auto Loader reads inventory, the
archive step only moves what was copied, the schedule fits the WMS deadline and the SLA check, and no
password or key is written anywhere.

Run locally with:
    python -m unittest tests.test_adf -v
"""

import json
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "infra"
ADF_TF = (INFRA / "adf.tf").read_text(encoding="utf-8")
SFTP_TF = (INFRA / "wms_sftp.tf").read_text(encoding="utf-8")
ACTIVITIES = json.loads((INFRA / "adf" / "copy_wms_inventory.activities.json").read_text(encoding="utf-8"))


def resource(text: str, kind: str, name: str) -> str:
    """The body of resource "kind" "name" { ... } (braces balanced)."""
    start = text.index(f'resource "{kind}" "{name}"')
    depth, i = 0, text.index("{", start)
    for j in range(i, len(text)):
        depth += {"{": 1, "}": -1}.get(text[j], 0)
        if depth == 0:
            return text[i:j + 1]
    raise AssertionError(f"unbalanced {kind}.{name}")


def activity(name: str) -> dict:
    return next(a for a in ACTIVITIES if a["name"] == name)


class TestPipeline(unittest.TestCase):

    def test_two_steps_copy_then_archive(self):
        self.assertEqual([a["name"] for a in ACTIVITIES], ["copy_to_landing", "archive_inbox"])
        self.assertEqual(activity("archive_inbox")["dependsOn"],
                         [{"activity": "copy_to_landing", "dependencyConditions": ["Succeeded"]}])

    def test_both_steps_read_the_same_files_of_the_inbox(self):
        """Same folder, pattern and cutoff: a file arriving mid-run is neither archived unlanded nor lost."""
        sources = [a["typeProperties"]["source"]["storeSettings"] for a in ACTIVITIES]
        for s in sources:
            self.assertEqual((s["type"], s["wildcardFolderPath"], s["wildcardFileName"], s["recursive"]),
                             ("SftpReadSettings", "inbox", "*.csv", False))
            self.assertEqual(s["modifiedDatetimeEnd"], {"value": "@pipeline().TriggerTime", "type": "Expression"})

    def test_only_the_archive_step_deletes_from_the_inbox(self):
        landing = activity("copy_to_landing")["typeProperties"]["source"]["storeSettings"]
        archive = activity("archive_inbox")["typeProperties"]["source"]["storeSettings"]
        self.assertNotIn("deleteFilesAfterCompletion", landing)
        self.assertIs(archive["deleteFilesAfterCompletion"], True)

    def test_files_are_copied_byte_for_byte_with_retries(self):
        for a in ACTIVITIES:
            self.assertEqual(a["typeProperties"]["source"]["type"], "BinarySource", a["name"])
            self.assertEqual(a["typeProperties"]["sink"]["type"], "BinarySink", a["name"])
            self.assertGreaterEqual(a["policy"]["retry"], 1, a["name"])

    def test_activities_use_datasets_defined_in_terraform(self):
        defined = set(re.findall(r'name\s*=\s*"(ds_\w+)"', ADF_TF))
        used = {ref["referenceName"] for a in ACTIVITIES for ref in a["inputs"] + a["outputs"]}
        self.assertEqual(used, defined)
        self.assertEqual(activity("copy_to_landing")["outputs"][0]["referenceName"], "ds_landing_inventory")
        self.assertEqual(activity("archive_inbox")["outputs"][0]["referenceName"], "ds_wms_archive")


class TestWhereFilesGo(unittest.TestCase):

    def test_landing_dataset_is_where_auto_loader_reads_inventory(self):
        body = resource(ADF_TF, "azurerm_data_factory_custom_dataset", "landing_inventory")
        self.assertIn('container = "landing", folderPath = "inventory"', body)
        loader = (ROOT / "scm_platform" / "ingest_bronze.py").read_text(encoding="utf-8")
        self.assertIn('f"abfss://landing@{storage_account}.dfs.core.windows.net"', loader)
        self.assertIn('landing_path = f"{landing_root.rstrip(\'/\')}/{feed_name}/"', loader)

    def test_inbox_and_archive_are_folders_of_the_sftp_drop(self):
        for name, folder in (("wms_inbox", "inbox"), ("wms_archive", "archive")):
            body = resource(ADF_TF, "azurerm_data_factory_custom_dataset", name)
            self.assertIn("azurerm_data_factory_linked_service_sftp.wms_drop.name", body)
            self.assertIn(f'folderPath = "{folder}"', body)


class TestSchedule(unittest.TestCase):

    def test_daily_at_0130_utc_before_the_wms_deadline_and_the_sla_check(self):
        body = resource(ADF_TF, "azurerm_data_factory_trigger_schedule", "wms_nightly")
        self.assertRegex(body, r'frequency\s*=\s*"Day"')
        self.assertRegex(body, r'time_zone\s*=\s*"UTC"')
        self.assertRegex(body, r"hours\s*=\s*\[1\]")
        self.assertRegex(body, r"minutes\s*=\s*\[30\]")
        copy_at = (1, 30)
        seed = (ROOT / "liquibase" / "changelog" / "changes" / "013-quarantine-lifecycle-and-ops.sql").read_text()
        deadline = tuple(map(int, re.search(r"\('wms_batch',\s*'(\d\d):(\d\d)'", seed).groups()))
        sla = yaml.safe_load((ROOT / "databricks" / "resources" / "scm_wms_sla_check.yml").read_text())
        _, minute, hour, *_ = sla["resources"]["jobs"]["scm_wms_sla_check"]["schedule"]["quartz_cron_expression"].split()
        self.assertLess(copy_at, deadline, "the copy must land the file before the WMS deadline")
        self.assertLess(deadline, (int(hour), int(minute)), "the SLA check runs after the deadline")


class TestSecurity(unittest.TestCase):

    def test_no_password_or_key_is_written_in_terraform(self):
        for name, text in (("adf.tf", ADF_TF), ("wms_sftp.tf", SFTP_TF)):
            self.assertIsNone(re.search(r'^\s*(password|account_key|connection_string)\s*=\s*"', text, re.M), name)
            self.assertNotIn("AccountKey=", text, name)

    def test_adf_reads_the_sftp_password_from_key_vault(self):
        body = resource(ADF_TF, "azurerm_data_factory_linked_service_sftp", "wms_drop")
        self.assertIn("key_vault_password", body)
        self.assertIn("azurerm_key_vault_secret.adf_sftp_password.name", body)
        self.assertNotRegex(body, r"^\s*password\s*=", "the password must not be on the linked service")

    def test_adf_writes_the_lake_with_its_identity_and_only_to_landing(self):
        lake = resource(ADF_TF, "azurerm_data_factory_linked_service_azure_blob_storage", "data_lake")
        self.assertRegex(lake, r"use_managed_identity\s*=\s*true")
        role = resource(ADF_TF, "azurerm_role_assignment", "adf_landing_writer")
        self.assertIn('azurerm_storage_container.containers["landing"].id', role)
        self.assertEqual(len(re.findall(r'"Storage Blob Data \w+"', ADF_TF)), 1, "one storage role, on landing")

    def test_each_sftp_user_has_only_what_it_needs(self):
        def permissions(user):
            body = resource(SFTP_TF, "azurerm_storage_account_local_user", user)
            return set(re.findall(r"(\w+)\s*=\s*true", body.split("permissions", 1)[1]))
        self.assertEqual(permissions("wms"), {"create", "write", "list"})          # drops files, cannot read or delete
        self.assertEqual(permissions("adf"), {"read", "list", "create", "write", "delete"})

    def test_both_sftp_passwords_go_to_key_vault(self):
        for user in ("wms", "adf"):
            self.assertRegex(SFTP_TF, rf"value\s*=\s*azurerm_storage_account_local_user\.{user}\.password")

    def test_the_data_lake_itself_has_no_sftp(self):
        storage = (INFRA / "storage.tf").read_text(encoding="utf-8")
        self.assertNotIn("sftp_enabled", storage)
        self.assertRegex(resource(SFTP_TF, "azurerm_storage_account", "wms_drop"), r"sftp_enabled\s*=\s*true")


if __name__ == "__main__":
    unittest.main()
