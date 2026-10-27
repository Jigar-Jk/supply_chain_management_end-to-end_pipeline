"""
test_governance.py – Data governance as code: PII masking views, Unity Catalog grants, audit queries.

- Masking: the deployed Gold directory views (liquibase/changelog/gold/07-08) run on hand-built Silver
  rows twice - as a pii_readers member and as everyone else (is_member() is localized to true/false).
- Grants: infra/governance.tf is read as text, so a widened grant fails here before terraform apply.
- Audit: the governance tiles (dashboards/governance/*.sql) run on a hand-built system.access.audit.

Run locally with:
    python -m unittest tests.test_governance -v
"""

import re
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if PYSPARK_AVAILABLE:
    from local.preview.sql_local import localize_sql

ROOT = Path(__file__).resolve().parent.parent
GOLD_DIR = ROOT / "liquibase" / "changelog" / "gold"
GOVERNANCE_DIR = ROOT / "dashboards" / "governance"
GOVERNANCE_TF = ROOT / "infra" / "governance.tf"


def spark_session(app):
    return (SparkSession.builder.master("local[1]").appName(app)
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestPiiMasking(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = spark_session("UnitTestPiiMasking")
        # C1 has an old and a current version (SCD2): only the current one may appear
        spark.createDataFrame(
            [("C1", "Old Name GmbH", "RETAIL", "DE", "Berlin", False),
             ("C1", "Acme Retail GmbH", "RETAIL", "DE", "Hamburg", True),
             ("C2", "Jane Doe", "B2C", "IN", "Pune", True)],
            "customer_id string, customer_name string, customer_segment string, country string, city string, "
            "is_current boolean").createOrReplaceTempView("dim_customer")
        spark.createDataFrame(
            [("K1", "DHL Freight", "EXPRESS", "ROAD", 0.95, True, "anna.meier@dhl-freight.de", True),
             ("K2", "No Mail Cargo", "STANDARD", "SEA", 0.90, True, None, True)],
            "carrier_id string, carrier_name string, service_level string, transport_mode string, "
            "contracted_otif_target double, is_active boolean, contact_email string, is_current boolean") \
            .createOrReplaceTempView("dim_carrier")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def view(self, file, member_of=()):
        sql = (GOLD_DIR / file).read_text(encoding="utf-8")
        name = re.search(r"VIEW gold\.(\w+)", sql).group(1)
        self.spark.sql(localize_sql(sql, member_of=member_of))
        return {r[0]: r.asDict() for r in self.spark.table(name).collect()}

    def test_customer_name_hidden_from_non_members(self):
        rows = self.view("07_vw_customer_directory.sql")
        self.assertEqual({r["customer_name"] for r in rows.values()}, {"***"})
        self.assertEqual(rows["C2"]["city"], "Pune")  # segment and location stay usable

    def test_customer_name_visible_to_pii_readers(self):
        rows = self.view("07_vw_customer_directory.sql", member_of=("pii_readers",))
        self.assertEqual(rows["C1"]["customer_name"], "Acme Retail GmbH")
        self.assertEqual(rows["C2"]["customer_name"], "Jane Doe")

    def test_membership_of_another_group_does_not_unmask(self):
        rows = self.view("07_vw_customer_directory.sql", member_of=("admins",))
        self.assertEqual(rows["C1"]["customer_name"], "***")

    def test_masking_keeps_every_row_and_only_current_versions(self):
        masked = self.view("07_vw_customer_directory.sql")
        clear = self.view("07_vw_customer_directory.sql", member_of=("pii_readers",))
        self.assertEqual(sorted(masked), sorted(clear))
        self.assertEqual(sorted(clear), ["C1", "C2"])
        self.assertEqual(clear["C1"]["city"], "Hamburg")

    def test_carrier_email_shows_only_the_domain(self):
        rows = self.view("08_vw_carrier_directory.sql")
        self.assertEqual(rows["K1"]["contact_email"], "***@dhl-freight.de")
        self.assertIsNone(rows["K2"]["contact_email"])  # missing stays missing, not "***@"

    def test_carrier_email_visible_to_pii_readers(self):
        rows = self.view("08_vw_carrier_directory.sql", member_of=("pii_readers",))
        self.assertEqual(rows["K1"]["contact_email"], "anna.meier@dhl-freight.de")

    def test_pii_columns_are_never_selected_unmasked(self):
        for file, column in [("07_vw_customer_directory.sql", "customer_name"),
                             ("08_vw_carrier_directory.sql", "contact_email")]:
            sql = (GOLD_DIR / file).read_text(encoding="utf-8")
            select_list = sql[sql.index("SELECT"):sql.index("FROM silver")]
            bare = [line for line in select_list.splitlines()
                    if re.fullmatch(rf"\s*{column},?\s*", line)]
            self.assertEqual(bare, [], f"{file} selects {column} without the is_member() mask")


def blocks(text, header_pattern):
    """(name, body) of every top-level block whose header matches, found by brace matching."""
    found = []
    for m in re.finditer(header_pattern, text):
        depth, i = 1, m.end()
        while depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        found.append((m.group(1), text[m.end():i - 1]))
    return found


def grants_of(resource_name):
    """{principal resource: privileges} of one databricks_grants resource in governance.tf."""
    text = re.sub(r"#.*", "", GOVERNANCE_TF.read_text(encoding="utf-8"))
    body = dict(blocks(text, r'resource "databricks_grants" "(\w+)" \{'))[resource_name]
    result = {}
    for _, grant in blocks(body, r"(grant) \{"):
        principal = re.search(r"databricks_service_principal\.(\w+)", grant).group(1)
        result[principal] = set(re.findall(r'"([A-Z_]+)"', re.search(r"privileges\s*=\s*\[(.*?)\]", grant,
                                                                       re.S).group(1)))
    return result


class TestGrantsAsCode(unittest.TestCase):

    def test_only_the_expected_grant_resources(self):
        text = GOVERNANCE_TF.read_text(encoding="utf-8")
        self.assertEqual(sorted(re.findall(r'resource "databricks_grants" "(\w+)"', text)),
                         ["catalog", "checkpoints", "gold", "landing"])

    def test_pipeline_files_access_is_least_privilege(self):
        self.assertEqual(grants_of("landing"), {"pipeline": {"READ_FILES"}})  # reads landing, never writes it
        self.assertEqual(grants_of("checkpoints"), {"pipeline": {"READ_FILES", "WRITE_FILES"}})
        text = GOVERNANCE_TF.read_text(encoding="utf-8")
        for location in ("rejected", "managed"):
            self.assertNotIn(f'locations["{location}"]', text)

    def test_pipeline_may_run_the_quarantine_alert(self):
        """The daily job evaluates the alert as scm-pipeline; without CAN_RUN the task cannot see it."""
        text = re.sub(r"#.*", "", (GOVERNANCE_TF.parent / "alerts.tf").read_text(encoding="utf-8"))
        perms = dict(blocks(text, r'resource "databricks_permissions" "(\w+)" \{'))
        for name, target in [("quarantine_rate_query", "sql_query_id"), ("quarantine_rate_alert", "sql_alert_id")]:
            self.assertIn(target, perms[name])
            self.assertIn("databricks_service_principal.pipeline.application_id", perms[name])
            self.assertIn('"CAN_RUN"', perms[name])

    def test_cd_secret_is_never_printed(self):
        outputs = (GOVERNANCE_TF.parent / "outputs.tf").read_text(encoding="utf-8")
        body = dict(blocks(outputs, r'output "(\w+)" \{'))["pipeline_client_secret"]
        self.assertRegex(body, r"sensitive\s*=\s*true")

    def test_analyst_can_only_enter_the_catalog(self):
        self.assertEqual(grants_of("catalog")["analyst"], {"USE_CATALOG"})

    def test_analyst_reads_gold_and_cannot_write(self):
        self.assertEqual(grants_of("gold")["analyst"], {"USE_SCHEMA", "SELECT"})

    def test_pipeline_owns_the_catalog_and_is_not_on_gold_separately(self):
        self.assertEqual(grants_of("catalog")["pipeline"], {"ALL_PRIVILEGES"})
        self.assertNotIn("pipeline", grants_of("gold"))  # inherited from the catalog

    def test_gold_grant_targets_the_gold_schema_of_every_catalog(self):
        text = GOVERNANCE_TF.read_text(encoding="utf-8")
        body = dict(blocks(text, r'resource "databricks_grants" "(\w+)" \{'))["gold"]
        self.assertIn("for_each = local.catalogs", body)
        self.assertIn('databricks_schema.medallion["${each.key}.gold"]', body)

    def test_pii_readers_group_is_created_empty(self):
        text = GOVERNANCE_TF.read_text(encoding="utf-8")
        self.assertIn('resource "databricks_group" "pii_readers"', text)
        self.assertNotIn("databricks_group_member", text)  # members are added by an admin, not by code


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark is required")
class TestAuditTiles(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        spark = cls.spark = spark_session("UnitTestAuditTiles")
        P, A = "scm_platform_dev", "analyst-app-id"
        events = [
            # (days ago, service, action, principal, request_params, status, error)
            (0, "unityCatalog", "getTable", A, {"full_name_arg": f"{P}.gold.vw_otif_daily"}, 200, None),
            (0, "unityCatalog", "generateTemporaryTableCredential", A,
             {"full_name_arg": f"{P}.gold.vw_otif_daily"}, 200, None),
            (0, "unityCatalog", "getTable", A, {"full_name_arg": f"{P}.silver.dim_customer"}, 403,
             "PERMISSION_DENIED: User does not have USE SCHEMA on Schema 'scm_platform_dev.silver'"),
            (1, "unityCatalog", "getTable", "dev@example.com", {"full_name_arg": "other_catalog.x.y"}, 200, None),
            (2, "clusters", "create", "dev@example.com", {}, 200, None),
            (10, "unityCatalog", "getTable", A, {"full_name_arg": f"{P}.gold.vw_fill_rate_daily"}, 200, None),
            (3, "unityCatalog", "updatePermissions", "dev@example.com",
             {"securable_type": "schema", "securable_full_name": f"{P}.gold", "changes": "[{add: [SELECT]}]"},
             200, None),
            (45, "unityCatalog", "updatePermissions", "dev@example.com",
             {"securable_type": "schema", "securable_full_name": f"{P}.silver", "changes": "[]"}, 200, None),
        ]
        spark.createDataFrame(
            events, "days_ago int, service_name string, action_name string, email string, "
                    "request_params map<string,string>, status_code int, error_message string") \
            .withColumn("event_time", F.expr("current_timestamp() - make_interval(0, 0, 0, days_ago)")) \
            .select("event_time", F.to_date("event_time").alias("event_date"), "service_name", "action_name",
                    F.struct("email").alias("user_identity"), "request_params",
                    F.struct("status_code", "error_message").alias("response")) \
            .createOrReplaceTempView("audit")
        # system.access.audit is a three-part name; a temp view can only have one
        cls.tiles = {p.stem: spark.sql(p.read_text(encoding="utf-8").replace("system.access.audit", "audit"))
                     .collect() for p in sorted(GOVERNANCE_DIR.glob("*.sql"))}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_all_three_tiles_exist(self):
        self.assertEqual(sorted(self.tiles), ["01_table_reads", "02_access_denied", "03_permission_changes"])

    def test_reads_cover_our_catalogs_last_seven_days_successful_only(self):
        got = {(r.principal, r.table_name): r.events for r in self.tiles["01_table_reads"]}
        self.assertEqual(got, {("analyst-app-id", "scm_platform_dev.gold.vw_otif_daily"): 2})

    def test_denied_request_names_the_object_and_reason(self):
        rows = self.tiles["02_access_denied"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].object_name, "scm_platform_dev.silver.dim_customer")
        self.assertIn("PERMISSION_DENIED", rows[0].error_message)

    def test_permission_changes_last_thirty_days(self):
        rows = self.tiles["03_permission_changes"]
        self.assertEqual([(r.changed_by, r.securable) for r in rows], [("dev@example.com", "scm_platform_dev.gold")])
