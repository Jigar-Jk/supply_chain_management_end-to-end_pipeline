"""
test_cdc_azure.py – SQL Server CDC through Azure Event Hubs, checked offline.

Debezium -> Event Hubs only runs against the live stack, so these tests read the Terraform, the
compose service, the connector configs and the script, and check they agree: the event hubs are the
topics Debezium writes and stream_cdc reads, Connect's internal topics exist and are compacted, the
Connect rule cannot manage, nothing secret is in a file, and the Azure connector differs from the
local one only where it must.

Run locally with:
    python -m unittest tests.test_cdc_azure -v
"""

import json
import re
import unittest
from pathlib import Path

import yaml

from scm_platform.stream_cdc_events import CDC_TABLES, TOPIC_PREFIX

ROOT = Path(__file__).resolve().parent.parent
TF = re.sub(r"#.*", "", (ROOT / "infra" / "cdc_eventhubs.tf").read_text(encoding="utf-8"))
COMPOSE = yaml.safe_load((ROOT / "local" / "streaming" / "docker-compose.yml").read_text(encoding="utf-8"))
CONNECT = COMPOSE["services"]["kafka-connect-azure"]
LOCAL_CONNECTOR = json.loads((ROOT / "cdc" / "debezium" / "sqlserver-connector.json").read_text(encoding="utf-8"))
AZURE_CONNECTOR = json.loads((ROOT / "cdc" / "debezium" / "sqlserver-connector.azure.json").read_text(encoding="utf-8"))
SCRIPT = (ROOT / "scripts" / "cdc_azure.ps1").read_text(encoding="utf-8")


def block(text: str, header: str) -> str:
    start = text.index(header)
    depth, i = 0, text.index("{", start)
    for j in range(i, len(text)):
        depth += {"{": 1, "}": -1}.get(text[j], 0)
        if depth == 0:
            return text[i:j + 1]
    raise AssertionError(f"unbalanced {header}")


def tf_map(name: str) -> dict:
    """{key: number} of a map in the locals block."""
    body = block(TF, f"{name} = {{")
    return {k: int(v) for k, v in re.findall(r'"([^"]+)"\s*=\s*(\d+)', body)}


class TestTopics(unittest.TestCase):

    def test_change_topics_are_the_ones_debezium_writes_and_stream_cdc_reads(self):
        database = LOCAL_CONNECTOR["database.names"]
        written = {f'{LOCAL_CONNECTOR["topic.prefix"]}.{database}.{t}'
                   for t in LOCAL_CONNECTOR["table.include.list"].split(",")}
        read = {f"{TOPIC_PREFIX}.{t}" for t in CDC_TABLES}
        self.assertEqual(written, read)
        self.assertEqual(set(tf_map("cdc_topics")), read)

    def test_connect_internal_topics_exist_and_are_compacted(self):
        env = CONNECT["environment"]
        names = {env["CONFIG_STORAGE_TOPIC"], env["OFFSET_STORAGE_TOPIC"], env["STATUS_STORAGE_TOPIC"]}
        topics = tf_map("connect_topics")
        self.assertEqual(set(topics), names)
        self.assertEqual(topics[env["CONFIG_STORAGE_TOPIC"]], 1)       # Connect requires one partition
        connect = block(TF, 'resource "azurerm_eventhub" "connect"')
        self.assertIn("for_each        = local.connect_topics", connect)
        self.assertRegex(connect, r'cleanup_policy\s*=\s*"Compact"')

    def test_change_topics_keep_as_long_as_the_cdc_change_tables(self):
        cdc = block(TF, 'resource "azurerm_eventhub" "cdc"')
        self.assertRegex(cdc, r"message_retention\s*=\s*7")
        enable = (ROOT / "cdc" / "sqlserver" / "02_enable_cdc.sql").read_text(encoding="utf-8")
        self.assertIn("10080", enable)                                  # 7 days in minutes

    def test_namespace_stays_within_ten_event_hubs(self):
        self.assertLessEqual(1 + len(tf_map("cdc_topics")) + len(tf_map("connect_topics")), 10)


class TestAccess(unittest.TestCase):

    def test_connect_rule_sends_and_listens_but_cannot_manage(self):
        rule = block(TF, 'resource "azurerm_eventhub_namespace_authorization_rule" "debezium_connect"')
        flags = dict(re.findall(r"(listen|send|manage)\s*=\s*(true|false)", rule))
        self.assertEqual(flags, {"listen": "true", "send": "true", "manage": "false"})

    def test_connection_string_only_in_key_vault(self):
        self.assertIn('name         = "eventhub-debezium-connection-string"', TF)
        self.assertNotIn("databricks_secret", TF)
        self.assertIn("--name eventhub-debezium-connection-string", SCRIPT)

    def test_no_secret_in_the_compose_file_or_connectors(self):
        env = CONNECT["environment"]
        self.assertIn("${EVENTHUB_DEBEZIUM_CONNECTION_STRING:-}", env["CONNECT_SASL_JAAS_CONFIG"])
        self.assertEqual(env["CONNECT_PRODUCER_SASL_JAAS_CONFIG"], env["CONNECT_SASL_JAAS_CONFIG"])
        self.assertEqual(env["CONNECT_CONSUMER_SASL_JAAS_CONFIG"], env["CONNECT_SASL_JAAS_CONFIG"])
        for connector in (LOCAL_CONNECTOR, AZURE_CONNECTOR):
            self.assertEqual(connector["database.password"], "${env:DEBEZIUM_SQL_PASSWORD}")

    def test_script_drops_the_secrets_from_its_window(self):
        self.assertIn("Remove-Item Env:EVENTHUB_DEBEZIUM_CONNECTION_STRING, Env:DEBEZIUM_SQL_PASSWORD", SCRIPT)
        self.assertNotRegex(SCRIPT, r"Write-Host[^\n]*CONNECTION_STRING")

    def test_every_client_speaks_sasl_ssl(self):
        env = CONNECT["environment"]
        for prefix in ("CONNECT_", "CONNECT_PRODUCER_", "CONNECT_CONSUMER_"):
            self.assertEqual(env[f"{prefix}SECURITY_PROTOCOL"], "SASL_SSL")
            self.assertEqual(env[f"{prefix}SASL_MECHANISM"], "PLAIN")


class TestConnector(unittest.TestCase):

    def test_azure_connector_differs_only_in_the_schema_history(self):
        def without_history(c):
            return {k: v for k, v in c.items() if not k.startswith("schema.history.internal")}
        self.assertEqual(without_history(AZURE_CONNECTOR), without_history(LOCAL_CONNECTOR))

    def test_schema_history_is_a_file_on_the_container_volume(self):
        self.assertEqual(AZURE_CONNECTOR["schema.history.internal"], "io.debezium.storage.file.history.FileSchemaHistory")
        volume, mount = CONNECT["volumes"][0].split(":")
        self.assertTrue(AZURE_CONNECTOR["schema.history.internal.file.filename"].startswith(mount + "/"))
        self.assertIn(volume, COMPOSE["volumes"])

    def test_azure_connect_is_opt_in_and_does_not_need_the_local_kafka(self):
        self.assertEqual(CONNECT["profiles"], ["azure"])
        self.assertNotIn("depends_on", CONNECT)

    def test_script_checks_sql_server_agent_first(self):
        self.assertLess(SCRIPT.index("SQLSERVERAGENT"), SCRIPT.index("up -d --wait kafka-connect-azure"))
        self.assertIn("sqlserver-connector.azure.json", SCRIPT)


if __name__ == "__main__":
    unittest.main()
